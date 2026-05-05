#pragma once
// sessions.hpp — Per-tenant session tracking and token-bucket rate limiter
//
// Phase 2: replaced sliding-window rate limiter with a proper token-bucket
// algorithm as specified in Section 8 of the project spec.
//
// Token bucket mechanics:
//   - Capacity = rate_limit_per_sec (one query costs one token)
//   - Refill rate = rate_limit_per_sec tokens/second
//   - Allows burst up to capacity when the tenant has been idle
//   - Rejected queries get an exact "retry in Xms" message

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include <mutex>
#include <atomic>
#include <cmath>
#include "config.hpp"

struct SessionResult {
    bool        ok;
    int         sessionId;
    std::string reason;
};

// Per-tenant token bucket state
struct TokenBucket {
    double     tokens;       // current token count (floating-point)
    std::chrono::steady_clock::time_point lastRefill;

    explicit TokenBucket(int capacity)
        : tokens(static_cast<double>(capacity)),
          lastRefill(std::chrono::steady_clock::now())
    {}
};

class SessionManager {
public:
    // --------------------------------------------------------
    // Session open / close (max_connections enforcement)
    // --------------------------------------------------------
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

    // --------------------------------------------------------
    // Token-bucket rate limiter (Section 8, per spec)
    //
    // Algorithm:
    //   1. elapsed = now - last_refill_time
    //   2. tokens  = min(tokens + elapsed_sec * rate, capacity)
    //   3. last_refill_time = now
    //   4. If tokens >= 1: subtract 1, accept.
    //      Else: compute retry delay, reject.
    // --------------------------------------------------------
    bool tryConsumeRate(const Tenant& tenant, std::string& reason) {
        using Clock    = std::chrono::steady_clock;
        using FpMillis = std::chrono::duration<double, std::milli>;

        const auto now = Clock::now();
        const double capacity = static_cast<double>(tenant.rateLimit);

        std::lock_guard<std::mutex> lock(mutex_);

        // Lazily initialise the bucket the first time this tenant sends a query
        auto it = buckets_.find(tenant.id);
        if (it == buckets_.end()) {
            buckets_.emplace(tenant.id, TokenBucket(tenant.rateLimit));
            it = buckets_.find(tenant.id);
            // Synchronize lastRefill to prevent negative elapsed time
            it->second.lastRefill = now;
        }
        TokenBucket& bucket = it->second;

        // Step 1-3: refill
        double elapsed_sec =
            std::chrono::duration<double>(now - bucket.lastRefill).count();
        bucket.tokens = std::min(
            bucket.tokens + elapsed_sec * capacity,
            capacity
        );
        bucket.lastRefill = now;

        // Step 4: consume or reject
        if (bucket.tokens >= 1.0) {
            bucket.tokens -= 1.0;
            reason.clear();
            return true;
        }

        // Compute time until the next token is available
        double tokensNeeded = 1.0 - bucket.tokens;
        double retryMs = (tokensNeeded / capacity) * 1000.0;
        long   retryMsL = static_cast<long>(std::ceil(retryMs));

        reason = "rate limit exceeded (" + std::to_string(tenant.rateLimit) +
                 " queries/sec, retry in " + std::to_string(retryMsL) + "ms)";
        return false;
    }

    // Return current effective rate (tokens consumed in last second) for \stats
    // We approximate this as (capacity - current_tokens) relative to last refill.
    // Simple display value only — not used for enforcement.
    double currentRate(const std::string& tenantId, int rateLimit) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = buckets_.find(tenantId);
        if (it == buckets_.end()) return 0.0;
        const TokenBucket& b = it->second;
        using Clock = std::chrono::steady_clock;
        double elapsed = std::chrono::duration<double>(Clock::now() - b.lastRefill).count();
        // Tokens currently available after hypothetical refill
        double projected = std::min(b.tokens + elapsed * rateLimit, (double)rateLimit);
        return std::max(0.0, (double)rateLimit - projected);
    }

private:
    std::mutex                                              mutex_;
    std::atomic<int>                                        nextSessionId_{ 1 };
    std::unordered_map<std::string, std::unordered_set<int>> activeSessions_;
    std::unordered_map<std::string, TokenBucket>            buckets_;
};
