#pragma once
// sessions.hpp — Per-tenant session tracking (thread-safe)
//
// Tracks how many sessions are currently open per tenant so we can
// enforce max_connections from the tenant config.
// One thread per client connection -> mutex required.

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <deque>
#include <chrono>
#include <mutex>
#include <atomic>
#include "config.hpp"

struct SessionResult {
    bool        ok;
    int         sessionId;
    std::string reason;
};

class SessionManager {
public:
    SessionResult openSession(const Tenant& tenant) {
        std::lock_guard<std::mutex> lock(mutex_);

        auto& sessions = activeSessions_[tenant.id];
        if ((int)sessions.size() >= tenant.maxConnections) {
            return { false, 0,
                "tenant \"" + tenant.id + "\" is at its connection limit (" +
                std::to_string(tenant.maxConnections) + ")"
            };
        }
        int sid = nextSessionId_++;
        sessions.insert(sid);
        return { true, sid, "" };
    }

    void closeSession(const std::string& tenantId, int sessionId) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = activeSessions_.find(tenantId);
        if (it != activeSessions_.end())
            it->second.erase(sessionId);
    }

    int sessionCount(const std::string& tenantId) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = activeSessions_.find(tenantId);
        if (it == activeSessions_.end()) return 0;
        return (int)it->second.size();
    }

    bool tryConsumeRate(const Tenant& tenant, std::string& reason) {
        using Clock = std::chrono::steady_clock;
        const auto now = Clock::now();
        const auto cutoff = now - std::chrono::seconds(1);

        std::lock_guard<std::mutex> lock(mutex_);
        auto& window = recentQueries_[tenant.id];

        while (!window.empty() && window.front() < cutoff) {
            window.pop_front();
        }

        if ((int)window.size() >= tenant.rateLimit) {
            reason = "rate limit exceeded for tenant \"" + tenant.id +
                     "\" (" + std::to_string(tenant.rateLimit) + "/sec)";
            return false;
        }

        window.push_back(now);
        reason.clear();
        return true;
    }

private:
    std::mutex mutex_;
    std::atomic<int> nextSessionId_{ 1 };
    std::unordered_map<std::string, std::unordered_set<int>> activeSessions_;
    std::unordered_map<std::string,
        std::deque<std::chrono::steady_clock::time_point>> recentQueries_;
};
