// tests/unit_test.cpp — Unit tests for denylist, config loader, and token bucket
//
// Phase 2: adds token-bucket rate limiter tests.
//
// Build:
//   g++ -std=c++17 -O2 -o tests/unit_test tests/unit_test.cpp -I src
// Run:
//   ./tests/unit_test

#include <iostream>
#include <thread>
#include <cassert>
#include <fstream>
#include <cstdlib>
#include <stdexcept>

#include "../src/rewriter.hpp"
#include "../src/config.hpp"
#include "../src/sessions.hpp"

int passed = 0, failed = 0;

#define TEST(name, expr)                                          \
    do {                                                          \
        try {                                                     \
            if (expr) { std::cout << "  \xE2\x9C\x93  " name "\n"; passed++; } \
            else       { std::cout << "  \xE2\x9C\x97  " name "\n"; failed++; } \
        } catch (const std::exception& ex) {                      \
            std::cout << "  \xE2\x9C\x97  " name " (exception: " << ex.what() << ")\n"; \
            failed++;                                             \
        }                                                         \
    } while(0)

#define TEST_THROWS(name, expr)                                   \
    do {                                                          \
        bool threw = false;                                       \
        try { expr; } catch (...) { threw = true; }               \
        if (threw) { std::cout << "  \xE2\x9C\x93  " name "\n"; passed++; } \
        else        { std::cout << "  \xE2\x9C\x97  " name "\n"; failed++; } \
    } while(0)

// Write a temporary config file and return its path
static std::string writeTempConf(const std::string& content) {
    std::string path = "/tmp/tenants_test_" + std::to_string(rand()) + ".conf";
    std::ofstream f(path);
    f << content;
    return path;
}

int main() {
    // -----------------------------------------------------------------------
    // Denylist tests
    // -----------------------------------------------------------------------
    std::cout << "\n-- Denylist / checkQuery -----------------------------------\n";

    // Allowed
    TEST("plain SELECT is allowed",
         checkQuery("SELECT * FROM users").allowed);
    TEST("CREATE TABLE is allowed",
         checkQuery("CREATE TABLE orders (id INT PRIMARY KEY)").allowed);
    TEST("INSERT is allowed",
         checkQuery("INSERT INTO users VALUES (1, 'Alice')").allowed);
    TEST("UPDATE is allowed",
         checkQuery("UPDATE users SET name='Bob' WHERE id=1").allowed);
    TEST("DELETE is allowed",
         checkQuery("DELETE FROM users WHERE id=1").allowed);
    TEST("JOIN on own tables is allowed",
         checkQuery("SELECT * FROM users u JOIN orders o ON u.id = o.user_id").allowed);
    TEST("SELECT 1 is allowed",
         checkQuery("SELECT 1").allowed);

    // Blocked
    TEST("explicit tenant_ schema reference is blocked",
         !checkQuery("SELECT * FROM tenant_globex.users").allowed);
    TEST("SET search_path is blocked (lowercase)",
         !checkQuery("set search_path = public").allowed);
    TEST("SET search_path is blocked (uppercase)",
         !checkQuery("SET SEARCH_PATH = tenant_acme").allowed);
    TEST("SET search_path is blocked (mixed case)",
         !checkQuery("Set Search_Path TO tenant_x").allowed);
    TEST("SET ROLE is blocked",
         !checkQuery("SET ROLE admin").allowed);
    TEST("set role is blocked (lowercase)",
         !checkQuery("set role superuser").allowed);
    TEST("cross-schema reference with spaces is blocked",
         !checkQuery("SELECT * FROM tenant_acme . users").allowed);
    TEST("attempted escape via subquery is blocked",
         !checkQuery("SELECT (SELECT * FROM tenant_other.secrets)").allowed);

    // -----------------------------------------------------------------------
    // Config loader tests
    // -----------------------------------------------------------------------
    std::cout << "\n-- loadTenants ---------------------------------------------\n";

    TEST("loads a valid config file", [&]() -> bool {
        auto f = writeTempConf("acme:acme_k3y:20:8\nglobex:globex_k3y:50:16\n");
        auto tenants = loadTenants(f);
        return tenants.size() == 2
            && tenants.at("acme").schema         == "tenant_acme"
            && tenants.at("acme").rateLimit      == 20
            && tenants.at("acme").maxConnections == 8
            && tenants.at("globex").apiKey       == "globex_k3y";
    }());

    TEST("skips comment and blank lines", [&]() -> bool {
        auto f = writeTempConf("# this is a comment\n\nacme:key:10:4\n");
        auto tenants = loadTenants(f);
        return tenants.size() == 1;
    }());

    TEST_THROWS("throws on wrong number of fields", {
        auto f = writeTempConf("acme:key:10\n");
        loadTenants(f);
    });

    TEST_THROWS("throws on duplicate tenant_id", {
        auto f = writeTempConf("acme:key:10:4\nacme:key2:20:8\n");
        loadTenants(f);
    });

    TEST_THROWS("throws on non-numeric rate limit", {
        auto f = writeTempConf("acme:key:abc:4\n");
        loadTenants(f);
    });

    TEST_THROWS("throws on zero max_connections", {
        auto f = writeTempConf("acme:key:10:0\n");
        loadTenants(f);
    });

    TEST_THROWS("throws on empty file", {
        auto f = writeTempConf("# just a comment\n");
        loadTenants(f);
    });

    // -----------------------------------------------------------------------
    // Token-bucket rate limiter tests
    // -----------------------------------------------------------------------
    std::cout << "\n-- Token-bucket rate limiter --------------------------------\n";

    // Build a minimal tenant config for testing
    auto makeTenant = [](const std::string& id, int rate) {
        Tenant t;
        t.id = id; t.apiKey = "key"; t.rateLimit = rate;
        t.maxConnections = 8; t.schema = "tenant_" + id;
        return t;
    };

    TEST("burst: all tokens consumed when bucket is full", [&]() -> bool {
        SessionManager sm;
        Tenant t = makeTenant("tb1", 5);
        std::string reason;
        // 5 queries should all succeed immediately (bucket starts full)
        bool ok = true;
        for (int i = 0; i < 5; i++)
            ok = ok && sm.tryConsumeRate(t, reason);
        return ok;
    }());

    TEST("rate limit: 6th query rejected when rate=5", [&]() -> bool {
        SessionManager sm;
        Tenant t = makeTenant("tb2", 5);
        std::string reason;
        for (int i = 0; i < 5; i++) sm.tryConsumeRate(t, reason);
        bool rejected = !sm.tryConsumeRate(t, reason);
        return rejected;
    }());

    TEST("rate limit: rejection message contains 'rate limit exceeded'", [&]() -> bool {
        SessionManager sm;
        Tenant t = makeTenant("tb3", 2);
        std::string reason;
        sm.tryConsumeRate(t, reason);
        sm.tryConsumeRate(t, reason);
        sm.tryConsumeRate(t, reason); // 3rd — should be rejected
        return reason.find("rate limit exceeded") != std::string::npos;
    }());

    TEST("rate limit: rejection message contains 'retry in'", [&]() -> bool {
        SessionManager sm;
        Tenant t = makeTenant("tb4", 2);
        std::string reason;
        sm.tryConsumeRate(t, reason);
        sm.tryConsumeRate(t, reason);
        sm.tryConsumeRate(t, reason);
        return reason.find("retry in") != std::string::npos;
    }());

    TEST("refill: tokens replenish after sleeping", [&]() -> bool {
        SessionManager sm;
        Tenant t = makeTenant("tb5", 2);
        std::string reason;
        sm.tryConsumeRate(t, reason);
        sm.tryConsumeRate(t, reason);
        bool rejectedBefore = !sm.tryConsumeRate(t, reason);
        // Sleep >1 second so bucket refills fully
        std::this_thread::sleep_for(std::chrono::milliseconds(1100));
        bool acceptedAfter = sm.tryConsumeRate(t, reason);
        return rejectedBefore && acceptedAfter;
    }());

    TEST("independent buckets: different tenants don't share tokens", [&]() -> bool {
        SessionManager sm;
        Tenant tA = makeTenant("tbA", 2);
        Tenant tB = makeTenant("tbB", 100);
        std::string reason;
        sm.tryConsumeRate(tA, reason);
        sm.tryConsumeRate(tA, reason);
        bool aRejected = !sm.tryConsumeRate(tA, reason);
        // tB should still have plenty of tokens
        bool bAllowed = sm.tryConsumeRate(tB, reason);
        return aRejected && bAllowed;
    }());

    TEST("rate=1: first query allowed, second rejected immediately", [&]() -> bool {
        SessionManager sm;
        Tenant t = makeTenant("tb6", 1);
        std::string reason;
        bool first  = sm.tryConsumeRate(t, reason);
        bool second = !sm.tryConsumeRate(t, reason);
        return first && second;
    }());

    // -----------------------------------------------------------------------
    std::cout << "\n-- Results -------------------------------------------------\n";
    std::cout << "   passed: " << passed << "\n";
    std::cout << "   failed: " << failed << "\n\n";

    return failed > 0 ? 1 : 0;
}
