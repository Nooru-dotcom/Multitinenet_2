// tenant-proxy.cpp — Phase 2 Multi-Tenant Database Proxy
//
// Phase 2 changes vs Phase 1:
//   - Per-tenant connection pool (TenantConnectionPool) replaces one-conn-per-query
//   - Token-bucket rate limiter replaces sliding-window approximation
//   - cpu_ms metering counter
//   - \stats shows real pool in-use / idle counts
//
// Build:
//   g++ -std=c++17 -O2 -pthread -o tenant-proxy src/tenant-proxy.cpp -lpq
//   (see Makefile)
//
// Usage:
//   ./tenant-proxy \
//     --port 6000 \
//     --backend localhost:5432 \
//     --backend-user postgres --backend-password secret \
//     --backend-db mydb \
//     --tenants tenants.conf

#include <iostream>
#include <string>
#include <unordered_map>
#include <thread>
#include <sstream>
#include <algorithm>
#include <cstring>

// POSIX networking
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>

#include "config.hpp"
#include "rewriter.hpp"
#include "sessions.hpp"
#include "backend.hpp"
#include "formatter.hpp"
#include "metrics.hpp"

// ============================================================
// Globals shared across all threads
// ============================================================
static TenantMap      g_tenants;
static Backend        g_backend;
static SessionManager g_sessions;
static MetricsStore   g_metrics;

// ============================================================
// Arg parsing
// ============================================================
struct Args {
    std::string port;
    std::string backendHost;
    int         backendPort = 5432;
    std::string backendUser;
    std::string backendPassword;
    std::string backendDb;
    std::string tenantsFile;
};

static void usage() {
    std::cerr <<
        "Usage: ./tenant-proxy\n"
        "  --port <port>\n"
        "  --backend <host:port>\n"
        "  --backend-user <user>\n"
        "  --backend-password <password>\n"
        "  --backend-db <dbname>\n"
        "  --tenants <tenants.conf>\n";
    std::exit(1);
}

static Args parseArgs(int argc, char* argv[]) {
    Args a;
    for (int i = 1; i < argc - 1; i += 2) {
        std::string key = argv[i];
        std::string val = argv[i + 1];
        if      (key == "--port")             a.port            = val;
        else if (key == "--backend") {
            auto colon = val.rfind(':');
            if (colon != std::string::npos) {
                a.backendHost = val.substr(0, colon);
                a.backendPort = std::stoi(val.substr(colon + 1));
            } else {
                a.backendHost = val;
            }
        }
        else if (key == "--backend-user")     a.backendUser     = val;
        else if (key == "--backend-password") a.backendPassword = val;
        else if (key == "--backend-db")       a.backendDb       = val;
        else if (key == "--tenants")          a.tenantsFile     = val;
    }
    if (a.port.empty() || a.backendHost.empty() || a.backendUser.empty() ||
        a.backendPassword.empty() || a.backendDb.empty() || a.tenantsFile.empty())
        usage();
    return a;
}

// ============================================================
// Socket I/O helpers
// ============================================================
static bool sendAll(int fd, const std::string& msg) {
    const char* buf = msg.c_str();
    size_t remaining = msg.size();
    while (remaining > 0) {
        ssize_t n = write(fd, buf, remaining);
        if (n <= 0) return false;
        buf       += n;
        remaining -= n;
    }
    return true;
}

static bool sendLine(int fd, const std::string& line) {
    return sendAll(fd, line + "\n");
}

// ============================================================
// Query execution — Phase 2 version uses connection pool
// ============================================================
static void execQuery(int clientFd, const Tenant& tenant,
                      const std::string& sql, int /*sessionId*/) {
    auto& metrics = g_metrics.get(tenant.id);

    // Denylist check (rewriter.hpp — unchanged from Phase 1)
    auto check = checkQuery(sql);
    if (!check.allowed) {
        metrics.queriesRejected++;
        sendLine(clientFd, formatErr(check.reason));
        return;
    }

    PGconn* conn    = nullptr;
    bool    healthy = false;
    try {
        // Checkout from per-tenant pool (blocks if at max_size)
        conn = g_backend.checkout(tenant.id);

        metrics.bytesSent += (long)sql.size();

        QueryResult qr = g_backend.runQuery(conn, sql);
        healthy = true;   // no exception → connection is still good

        // Update metering
        for (const auto& row : qr.rows)
            for (const auto& cell : row)
                metrics.bytesReceived += (long)cell.size();
        metrics.queriesTotal++;
        metrics.cpuMs += qr.durationMs;
        metrics.setLastQuery(sql);

        // Build pool stats for response footer
        int poolInUse = 0, poolIdle = 0;
        g_backend.poolStats(tenant.id, poolInUse, poolIdle);
        int poolTotal = poolInUse + poolIdle;

        std::string resp = qr.fields.empty()
            ? formatOk(qr.rowCount, qr.durationMs, poolInUse, poolTotal)
            : formatSelect(qr, poolInUse, poolTotal);

        sendLine(clientFd, resp);

    } catch (const std::exception& ex) {
        metrics.queriesRejected++;
        sendLine(clientFd, formatErr(ex.what()));
        // healthy stays false → pool will discard this connection
    }

    if (conn) g_backend.checkin(tenant.id, conn, healthy);
}

static bool enforceRateLimit(int clientFd, const Tenant& tenant) {
    std::string reason;
    if (g_sessions.tryConsumeRate(tenant, reason)) return true;
    g_metrics.get(tenant.id).queriesRejected++;
    sendLine(clientFd, formatErr(reason));
    return false;
}

// ============================================================
// Per-client handler (runs in its own detached thread)
// ============================================================
static void handleClient(int clientFd) {
    bool          authenticated = false;
    const Tenant* tenant        = nullptr;
    int           sessionId     = 0;
    std::string   lineBuffer;

    char buf[4096];
    while (true) {
        ssize_t n = read(clientFd, buf, sizeof(buf));
        if (n <= 0) break;

        lineBuffer.append(buf, n);

        size_t pos;
        while ((pos = lineBuffer.find('\n')) != std::string::npos) {
            std::string line = lineBuffer.substr(0, pos);
            lineBuffer.erase(0, pos + 1);

            // Trim \r (Windows) and leading/trailing whitespace
            if (!line.empty() && line.back() == '\r') line.pop_back();
            auto s = line.find_first_not_of(" \t");
            if (s == std::string::npos) continue;
            auto e = line.find_last_not_of(" \t");
            line = line.substr(s, e - s + 1);
            if (line.empty()) continue;

            // ----------------------------------------------------------
            // HELLO — must be first command
            // ----------------------------------------------------------
            if (!authenticated) {
                if (line.substr(0, 6) != "HELLO ") {
                    sendLine(clientFd, "ERR must send HELLO <tenant_id> <api_key> first");
                    goto disconnect;
                }
                std::istringstream iss(line);
                std::string cmd, tenantId, apiKey;
                iss >> cmd >> tenantId >> apiKey;
                if (tenantId.empty() || apiKey.empty()) {
                    sendLine(clientFd, "ERR usage: HELLO <tenant_id> <api_key>");
                    goto disconnect;
                }

                auto it = g_tenants.find(tenantId);
                if (it == g_tenants.end()) {
                    sendLine(clientFd, "ERR unknown tenant");
                    goto disconnect;
                }
                if (it->second.apiKey != apiKey) {
                    sendLine(clientFd, "ERR bad credentials");
                    goto disconnect;
                }

                auto result = g_sessions.openSession(it->second);
                if (!result.ok) {
                    sendLine(clientFd, "ERR " + result.reason);
                    goto disconnect;
                }

                tenant        = &it->second;
                sessionId     = result.sessionId;
                authenticated = true;

                sendLine(clientFd, "OK " + std::to_string(sessionId));
                std::cout << "[proxy] tenant \"" << tenant->id
                          << "\" connected (session " << sessionId << ")" << std::endl;
                continue;
            }

            // ----------------------------------------------------------
            // Authenticated commands
            // ----------------------------------------------------------
            if (line == "QUIT") {
                sendLine(clientFd, "OK bye");
                goto disconnect;
            }

            if (line == "\\stats") {
                auto& m = g_metrics.get(tenant->id);
                int openCount = g_sessions.sessionCount(tenant->id);
                int poolInUse = 0, poolIdle = 0;
                g_backend.poolStats(tenant->id, poolInUse, poolIdle);
                double curRate = g_sessions.currentRate(tenant->id, tenant->rateLimit);

                std::ostringstream oss;
                oss << "tenant:           " << tenant->id          << "\n"
                    << "session id:       " << sessionId           << "\n"
                    << "queries total:    " << m.queriesTotal       << "\n"
                    << "queries rejected: " << m.queriesRejected    << "\n"
                    << "bytes sent:       " << m.bytesSent          << "\n"
                    << "bytes recv:       " << m.bytesReceived      << "\n"
                    << "cpu ms:           " << m.cpuMs              << "\n"
                    << "pool in use:      " << poolInUse            << "\n"
                    << "pool idle:        " << poolIdle             << "\n"
                    << "pool max:         " << tenant->maxConnections << "\n"
                    << "open sessions:    " << openCount << " / "
                                           << tenant->maxConnections << "\n"
                    << "rate limit:       " << tenant->rateLimit
                                           << "/sec (current: "
                                           << static_cast<int>(curRate) << "/sec)\n"
                    << "last query:       " << m.getLastQuery();
                sendLine(clientFd, oss.str());
                continue;
            }

            if (line == "\\tables") {
                std::string sql =
                    "SELECT tablename FROM pg_tables WHERE schemaname = '" +
                    tenant->schema + "'";
                if (!enforceRateLimit(clientFd, *tenant)) continue;
                execQuery(clientFd, *tenant, sql, sessionId);
                continue;
            }

            if (line.size() > 6 && line.substr(0, 6) == "QUERY ") {
                std::string sql = line.substr(6);
                auto ss = sql.find_first_not_of(" \t");
                if (ss == std::string::npos || sql.empty()) {
                    sendLine(clientFd, "ERR empty query");
                    continue;
                }
                sql = sql.substr(ss);
                if (!enforceRateLimit(clientFd, *tenant)) continue;
                execQuery(clientFd, *tenant, sql, sessionId);
                continue;
            }

            // Unknown command
            std::string cmd = line.substr(0, line.find(' '));
            sendLine(clientFd, "ERR unknown command: " + cmd);
        }
    }

disconnect:
    if (tenant && sessionId) {
        g_sessions.closeSession(tenant->id, sessionId);
        std::cout << "[proxy] tenant \"" << tenant->id
                  << "\" disconnected (session " << sessionId << ")" << std::endl;
    }
    close(clientFd);
}

// ============================================================
// Main
// ============================================================
int main(int argc, char* argv[]) {
    Args args = parseArgs(argc, argv);

    // Load tenants
    try {
        g_tenants = loadTenants(args.tenantsFile);
    } catch (const std::exception& ex) {
        std::cerr << "[proxy] ERROR loading tenants: " << ex.what() << "\n";
        return 1;
    }

    // Print tenant list
    std::cout << "[proxy] loaded " << g_tenants.size() << " tenants: ";
    bool first = true;
    for (const auto& [id, _] : g_tenants) {
        if (!first) std::cout << ", ";
        std::cout << id;
        first = false;
    }
    std::cout << "\n";

    // Init metrics for each tenant
    for (const auto& [id, _] : g_tenants)
        g_metrics.registerTenant(id);

    // Init backend config
    BackendConfig bcfg;
    bcfg.host     = args.backendHost;
    bcfg.port     = args.backendPort;
    bcfg.user     = args.backendUser;
    bcfg.password = args.backendPassword;
    bcfg.database = args.backendDb;
    g_backend.init(bcfg);

    // Create schemas
    std::cout << "[proxy] ensuring schemas exist on backend...\n";
    try {
        g_backend.ensureSchemas(g_tenants);
    } catch (const std::exception& ex) {
        std::cerr << "[proxy] ERROR creating schemas: " << ex.what() << "\n";
        return 1;
    }

    // Initialize per-tenant connection pools (Phase 2)
    std::cout << "[proxy] initializing connection pools...\n";
    try {
        g_backend.initPools(g_tenants);
    } catch (const std::exception& ex) {
        std::cerr << "[proxy] ERROR initializing pools: " << ex.what() << "\n";
        return 1;
    }
    std::cout << "[proxy] connection pool: min 1 per tenant\n";

    // Create TCP server socket
    int serverFd = socket(AF_INET, SOCK_STREAM, 0);
    if (serverFd < 0) { perror("socket"); return 1; }

    int opt = 1;
    setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons((uint16_t)std::stoi(args.port));

    if (bind(serverFd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(serverFd, 128) < 0) {
        perror("listen"); return 1;
    }

    std::cout << "[proxy] ready on port " << args.port << "\n";
    std::cout.flush();

    // Accept loop — one detached thread per client
    while (true) {
        sockaddr_in clientAddr{};
        socklen_t   clientLen = sizeof(clientAddr);
        int clientFd = accept(serverFd, (sockaddr*)&clientAddr, &clientLen);
        if (clientFd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }
        std::thread([clientFd]() { handleClient(clientFd); }).detach();
    }

    close(serverFd);
    return 0;
}
