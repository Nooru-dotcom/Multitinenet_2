#pragma once
// rewriter.hpp — Query denylist checker (Phase 1 baseline)
//
// Uses SET search_path per tenant connection. We reject queries that try
// to escape their schema via explicit schema-qualified references or
// dangerous SET commands.
//
// Denylist rules:
//   1. Reject: tenant_<x>.<something>   (explicit cross-schema reference)
//   2. Reject: SET search_path          (case-insensitive)
//   3. Reject: SET ROLE                 (case-insensitive)
//
// Limitation: regex-based, does not parse SQL. Patterns inside string
// literals or comments may be incorrectly matched/missed. Sufficient
// for the base project scope (see spec Section 7.3).

#include <string>
#include <regex>

struct CheckResult {
    bool        allowed;
    std::string reason;
};

// Patterns compiled once at program start
static const std::regex RE_TENANT_REF(
    R"(tenant_[a-z0-9_]+\s*\.\s*\S+)",
    std::regex::icase
);
static const std::regex RE_SET_SEARCH_PATH(
    R"(\bset\s+search_path\b)",
    std::regex::icase
);
static const std::regex RE_SET_ROLE(
    R"(\bset\s+role\b)",
    std::regex::icase
);

inline CheckResult checkQuery(const std::string& sql) {
    if (std::regex_search(sql, RE_TENANT_REF))
        return { false, "explicit cross-schema reference (tenant_X.something) is not allowed" };
    if (std::regex_search(sql, RE_SET_SEARCH_PATH))
        return { false, "SET search_path is not allowed" };
    if (std::regex_search(sql, RE_SET_ROLE))
        return { false, "SET ROLE is not allowed" };
    return { true, "" };
}
