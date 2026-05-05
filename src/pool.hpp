#pragma once
// pool.hpp — Per-tenant PostgreSQL connection pool (Phase 2)
//
// Design: one pool per tenant, each connection permanently bound to the
// tenant's schema via SET search_path. Connections are never shared across
// tenants, making isolation trivially correct.
//
// Pool parameters:
//   min_size    — connections kept alive even when idle (pre-warmed at startup)
//   max_size    — hard upper bound; checkout blocks when this is reached
//
// Thread safety: all state guarded by a single mutex + condition_variable.
//
// Include order requirement:
//   pool.hpp must be included AFTER backendcfg.hpp is visible and AFTER the
//   two free helper functions (buildConnStr, openAndBindConn) are defined.
//   backend.hpp satisfies this: it defines the helpers, then includes pool.hpp.

#include <string>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <stdexcept>
#include <iostream>
#include <libpq-fe.h>
#include "backendcfg.hpp"

// These two helpers are defined inline in backend.hpp (which includes this file).
// Declaring them here allows pool.hpp to call them.
std::string buildConnStr(const BackendConfig& cfg);
PGconn* openAndBindConn(const BackendConfig& cfg, const std::string& schema);


class TenantConnectionPool {
public:
    // cfg       — backend credentials (host, port, user, password, db)
    // schema    — tenant schema name, e.g. "tenant_acme"
    // minSize   — pre-warm this many connections at construction time
    // maxSize   — hard cap; checkout() blocks when in_use_ == maxSize
    TenantConnectionPool(const BackendConfig& cfg,
                         const std::string&   schema,
                         int                  minSize,
                         int                  maxSize)
        : cfg_(cfg), schema_(schema),
          minSize_(minSize), maxSize_(maxSize),
          inUse_(0)
    {
        // Pre-warm min_size connections at startup so the first queries are fast
        for (int i = 0; i < minSize_; i++) {
            PGconn* c = openAndBindConn(cfg_, schema_);
            idle_.push_back(c);
        }
    }

    ~TenantConnectionPool() {
        std::lock_guard<std::mutex> lock(mutex_);
        for (PGconn* c : idle_) PQfinish(c);
        idle_.clear();
    }

    // Non-copyable, non-movable (owns raw pointers)
    TenantConnectionPool(const TenantConnectionPool&)            = delete;
    TenantConnectionPool& operator=(const TenantConnectionPool&) = delete;

    // Check out one connection for a query.
    // Blocks if the pool is at max_size and no idle connection is available.
    PGconn* checkout() {
        std::unique_lock<std::mutex> lock(mutex_);

        // Wait until either an idle connection is available, or we are below
        // max_size and can open a new one.
        cv_.wait(lock, [this] {
            return !idle_.empty() || (inUse_ < maxSize_);
        });

        if (!idle_.empty()) {
            PGconn* c = idle_.front();
            idle_.pop_front();
            inUse_++;
            return c;
        }

        // Pool is below max_size — open a new connection.
        // Release the mutex during the potentially slow TCP connect.
        lock.unlock();
        PGconn* c = openAndBindConn(cfg_, schema_);
        lock.lock();
        inUse_++;
        return c;
    }

    // Return a connection to the pool.
    // healthy = true  → put back in idle list, notify waiters
    // healthy = false → discard; replenish below min_size if needed
    void checkin(PGconn* conn, bool healthy) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            inUse_--;
            if (healthy) {
                idle_.push_back(conn);
                conn = nullptr;
            }
            // If !healthy, conn stays set and is closed below the mutex
        }

        if (conn) PQfinish(conn);  // close broken connection outside mutex

        replenishIfNeeded();
        cv_.notify_one();          // wake a waiter that may have been blocked
    }

    // Read pool statistics — used by \stats command
    void stats(int& outInUse, int& outIdle) const {
        std::lock_guard<std::mutex> lock(mutex_);
        outInUse = inUse_;
        outIdle  = (int)idle_.size();
    }

    int maxSize() const { return maxSize_; }

private:
    BackendConfig               cfg_;
    std::string                 schema_;
    int                         minSize_;
    int                         maxSize_;

    mutable std::mutex          mutex_;
    std::condition_variable     cv_;
    std::deque<PGconn*>         idle_;
    int                         inUse_;

    // Open replacement connections until total (idle + in_use) >= min_size.
    // Called after a broken connection is discarded.
    void replenishIfNeeded() {
        std::unique_lock<std::mutex> lock(mutex_);
        while (inUse_ + (int)idle_.size() < minSize_) {
            lock.unlock();
            PGconn* c = nullptr;
            try {
                c = openAndBindConn(cfg_, schema_);
            } catch (const std::exception& ex) {
                std::cerr << "[pool] WARNING: failed to replenish connection for "
                          << schema_ << ": " << ex.what() << "\n";
                return;
            }
            lock.lock();
            idle_.push_back(c);
        }
    }
};
