#pragma once
// config.hpp — Tenant configuration loader
// Reads tenants.conf: tenant_id:api_key:rate_limit_per_sec:max_connections

#include <string>
#include <unordered_map>
#include <stdexcept>
#include <fstream>
#include <sstream>
#include <regex>
#include <algorithm>

struct Tenant {
    std::string id;
    std::string apiKey;
    int         rateLimit;       // queries/sec
    int         maxConnections;
    std::string schema;          // "tenant_<id>"
};

// Map: tenant_id -> Tenant
using TenantMap = std::unordered_map<std::string, Tenant>;

// trim whitespace from both ends
static inline std::string trim(const std::string& s) {
    auto start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    auto end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

// split string by a delimiter character into at most 4 parts
static inline std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> parts;
    std::stringstream ss(s);
    std::string token;
    while (std::getline(ss, token, delim)) {
        parts.push_back(token);
    }
    return parts;
}

// Validate tenant_id: lowercase alphanumeric + underscore only
static inline bool validTenantId(const std::string& id) {
    if (id.empty()) return false;
    for (char c : id) {
        if (!std::islower((unsigned char)c) && !std::isdigit((unsigned char)c) && c != '_')
            return false;
    }
    return true;
}

inline TenantMap loadTenants(const std::string& filePath) {
    std::ifstream file(filePath);
    if (!file.is_open())
        throw std::runtime_error("Cannot open tenants config file: " + filePath);

    TenantMap tenants;
    std::string line;
    int lineNum = 0;

    while (std::getline(file, line)) {
        lineNum++;
        line = trim(line);

        // Skip empty lines and comments
        if (line.empty() || line[0] == '#') continue;

        auto parts = split(line, ':');
        if (parts.size() != 4) {
            throw std::runtime_error(
                "tenants.conf line " + std::to_string(lineNum) +
                ": expected 4 colon-separated fields, got " +
                std::to_string(parts.size()) + ": \"" + line + "\""
            );
        }

        std::string tenantId  = trim(parts[0]);
        std::string apiKey    = trim(parts[1]);
        std::string rateLimS  = trim(parts[2]);
        std::string maxConnS  = trim(parts[3]);

        if (!validTenantId(tenantId)) {
            throw std::runtime_error(
                "tenants.conf line " + std::to_string(lineNum) +
                ": tenant_id \"" + tenantId + "\" must be lowercase alphanumeric"
            );
        }

        int rateLimit = 0, maxConnections = 0;
        try { rateLimit = std::stoi(rateLimS); } catch(...) { rateLimit = 0; }
        try { maxConnections = std::stoi(maxConnS); } catch(...) { maxConnections = 0; }

        if (rateLimit <= 0) {
            throw std::runtime_error(
                "tenants.conf line " + std::to_string(lineNum) +
                ": rate_limit_per_sec must be a positive integer, got \"" + rateLimS + "\""
            );
        }
        if (maxConnections <= 0) {
            throw std::runtime_error(
                "tenants.conf line " + std::to_string(lineNum) +
                ": max_connections must be a positive integer, got \"" + maxConnS + "\""
            );
        }
        if (tenants.count(tenantId)) {
            throw std::runtime_error(
                "tenants.conf line " + std::to_string(lineNum) +
                ": duplicate tenant_id \"" + tenantId + "\""
            );
        }

        Tenant t;
        t.id             = tenantId;
        t.apiKey         = apiKey;
        t.rateLimit      = rateLimit;
        t.maxConnections = maxConnections;
        t.schema         = "tenant_" + tenantId;
        tenants[tenantId] = t;
    }

    if (tenants.empty())
        throw std::runtime_error("tenants.conf is empty -- at least one tenant is required");

    return tenants;
}
