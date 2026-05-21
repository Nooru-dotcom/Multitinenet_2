#pragma once
// metrics.hpp — Per-tenant usage counters (thread-safe)
//
// Phase 2 adds cpu_ms (total wall-clock time spent executing queries).

#include <string>
#include <unordered_map>
#include <mutex>
#include <atomic>

struct TenantMetrics {
    std::atomic<long> queriesTotal    { 0 };
    std::atomic<long> queriesRejected { 0 };
    std::atomic<long> bytesSent       { 0 };
    std::atomic<long> bytesReceived   { 0 };
    std::atomic<long> cpuMs           { 0 };  // Phase 2: wall-clock query time

    std::string lastQuery;
    std::mutex  lastQueryMutex;

    void setLastQuery(const std::string& q) {
        std::lock_guard<std::mutex> lock(lastQueryMutex);
        lastQuery = q.size() > 60 ? q.substr(0, 60) : q;
    }
    std::string getLastQuery() {
        std::lock_guard<std::mutex> lock(lastQueryMutex);
        return lastQuery;
    }
};

class MetricsStore {
public:
    // Must be called at startup for each tenant before any query runs
    void registerTenant(const std::string& id) {
        std::lock_guard<std::mutex> lock(mutex_);
        store_[id] = std::make_unique<TenantMetrics>();
    }

    TenantMetrics& get(const std::string& id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return *store_.at(id);
    }

private:
    std::mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<TenantMetrics>> store_;
};
