#pragma once
// backend.hpp — PostgreSQL backend via libpq (Phase 2: per-tenant connection pool)
//
// Include order:
//   1. backendcfg.hpp  — BackendConfig POD
//   2. pool.hpp        — TenantConnectionPool (uses BackendConfig and the two helpers)
//   3. This file       — Backend class, defines buildConnStr/openAndBindConn inline
//
// The two free helpers (buildConnStr, openAndBindConn) are declared in pool.hpp
// and defined inline here. Because pool.hpp is included after their inline
// definitions, the linker sees exactly one definition.

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <stdexcept>
#include <chrono>
#include <iostream>
#include <libpq-fe.h>
#include "config.hpp"
#include "backendcfg.hpp"

// ============================================================
// RAII wrappers
// ============================================================
class PgConn {
public:
    explicit PgConn(PGconn* conn) : conn_(conn) {}
    ~PgConn() { if (conn_) PQfinish(conn_); }
    PgConn(const PgConn&) = delete;
    PgConn& operator=(const PgConn&) = delete;
    PGconn* get() { return conn_; }
private:
    PGconn* conn_;
};

class PgResult {
public:
    explicit PgResult(PGresult* res) : res_(res) {}
    ~PgResult() { if (res_) PQclear(res_); }
    PgResult(const PgResult&) = delete;
    PgResult& operator=(const PgResult&) = delete;
    PGresult* get() { return res_; }
private:
    PGresult* res_;
};

// ============================================================
// Free helper functions — declared in pool.hpp, defined here.
// pool.hpp is included AFTER these definitions so it resolves correctly.
// ============================================================
inline std::string buildConnStr(const BackendConfig& cfg) {
    return "host="      + cfg.host     +
           " port="     + std::to_string(cfg.port) +
           " user="     + cfg.user     +
           " password=" + cfg.password +
           " dbname="   + cfg.database;
}

inline PGconn* openAndBindConn(const BackendConfig& cfg, const std::string& schema) {
    std::string connStr = buildConnStr(cfg);
    PGconn* conn = PQconnectdb(connStr.c_str());
    if (!conn || PQstatus(conn) != CONNECTION_OK) {
        std::string err = conn ? PQerrorMessage(conn) : "out of memory";
        if (conn) PQfinish(conn);
        throw std::runtime_error("PostgreSQL connection failed: " + err);
    }
    // Permanently bind this connection to the tenant's schema.
    // Done once at pool creation; the setting is sticky for the connection's lifetime.
    std::string setPath = "SET search_path = " + schema;
    PGresult* res = PQexec(conn, setPath.c_str());
    ExecStatusType st = PQresultStatus(res);
    PQclear(res);
    if (st != PGRES_COMMAND_OK) {
        std::string err = PQerrorMessage(conn);
        PQfinish(conn);
        throw std::runtime_error("SET search_path failed for " + schema + ": " + err);
    }
    return conn;
}

// pool.hpp uses BackendConfig (now fully defined) and the two helpers (just defined)
#include "pool.hpp"

// ============================================================
// Result of a single query execution
// ============================================================
struct QueryResult {
    std::vector<std::string>              fields;
    std::vector<std::vector<std::string>> rows;
    int                                   rowCount;
    long                                  durationMs;
};

// ============================================================
// Backend — manages per-tenant pools and admin schema operations
// ============================================================
class Backend {
public:
    void init(const BackendConfig& cfg) { cfg_ = cfg; }

    // Create all tenant schemas at proxy startup (one-off admin connection)
    void ensureSchemas(const TenantMap& tenants) {
        PgConn admin(rawConnect());
        for (const auto& [id, tenant] : tenants) {
            std::string sql = "CREATE SCHEMA IF NOT EXISTS " + tenant.schema;
            PgResult res(PQexec(admin.get(), sql.c_str()));
            if (PQresultStatus(res.get()) != PGRES_COMMAND_OK) {
                throw std::runtime_error(
                    "Failed to create schema " + tenant.schema + ": " +
                    PQerrorMessage(admin.get())
                );
            }
            std::cout << "[proxy]   schema " << tenant.schema << ": ok" << std::endl;
        }
    }

    // Initialise per-tenant connection pools (call after ensureSchemas)
    void initPools(const TenantMap& tenants) {
        constexpr int MIN_SIZE = 1;
        for (const auto& [id, tenant] : tenants) {
            int maxSize = tenant.maxConnections;
            pools_[id] = std::make_unique<TenantConnectionPool>(
                cfg_, tenant.schema, MIN_SIZE, maxSize
            );
            std::cout << "[proxy]   pool for " << id
                      << ": min=" << MIN_SIZE
                      << " max=" << maxSize << "\n";
        }
    }

    // Check out a connection from the tenant's pool (blocks if at capacity)
    PGconn* checkout(const std::string& tenantId) {
        return pools_.at(tenantId)->checkout();
    }

    // Return a connection to the tenant's pool
    void checkin(const std::string& tenantId, PGconn* conn, bool healthy) {
        pools_.at(tenantId)->checkin(conn, healthy);
    }

    // Get pool stats for \stats command
    void poolStats(const std::string& tenantId, int& inUse, int& idle) const {
        pools_.at(tenantId)->stats(inUse, idle);
    }

    int poolMaxSize(const std::string& tenantId) const {
        return pools_.at(tenantId)->maxSize();
    }

    // Execute SQL on an existing connection; throws std::runtime_error on failure.
    QueryResult runQuery(PGconn* conn, const std::string& sql) {
        auto t0 = std::chrono::steady_clock::now();
        PgResult res(PQexec(conn, sql.c_str()));
        auto t1 = std::chrono::steady_clock::now();
        long ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

        ExecStatusType status = PQresultStatus(res.get());

        if (status == PGRES_FATAL_ERROR || status == PGRES_NONFATAL_ERROR) {
            std::string msg = PQresultErrorMessage(res.get());
            auto pos = msg.find("ERROR:");
            if (pos != std::string::npos) msg = msg.substr(pos + 6);
            auto s = msg.find_first_not_of(" \t\r\n");
            if (s != std::string::npos) msg = msg.substr(s);
            throw std::runtime_error(msg);
        }

        QueryResult qr;
        qr.durationMs = ms;

        if (status == PGRES_TUPLES_OK) {
            int ncols = PQnfields(res.get());
            int nrows = PQntuples(res.get());
            for (int c = 0; c < ncols; c++)
                qr.fields.push_back(PQfname(res.get(), c));
            for (int r = 0; r < nrows; r++) {
                std::vector<std::string> row;
                for (int c = 0; c < ncols; c++) {
                    if (PQgetisnull(res.get(), r, c))
                        row.push_back("NULL");
                    else
                        row.push_back(PQgetvalue(res.get(), r, c));
                }
                qr.rows.push_back(row);
            }
            qr.rowCount = nrows;
        } else {
            const char* affected = PQcmdTuples(res.get());
            qr.rowCount = (affected && *affected) ? std::stoi(affected) : 0;
        }

        return qr;
    }

private:
    BackendConfig cfg_;
    std::unordered_map<std::string, std::unique_ptr<TenantConnectionPool>> pools_;

    // Open a raw admin connection (no schema bound — used for CREATE SCHEMA)
    PGconn* rawConnect() {
        PGconn* conn = PQconnectdb(buildConnStr(cfg_).c_str());
        if (!conn || PQstatus(conn) != CONNECTION_OK) {
            std::string err = conn ? PQerrorMessage(conn) : "out of memory";
            if (conn) PQfinish(conn);
            throw std::runtime_error("PostgreSQL connection failed: " + err);
        }
        return conn;
    }
};
