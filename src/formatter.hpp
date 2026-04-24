#pragma once
// formatter.hpp — Format query results into the proxy text protocol

#include <string>
#include <vector>
#include <sstream>
#include <algorithm>
#include "backend.hpp"

// Remove leading "ERROR: " prefix that PostgreSQL adds
static inline std::string stripErrorPrefix(const std::string& msg) {
    std::string s = msg;
    // Case-insensitive search for "error:"
    std::string low = s;
    std::transform(low.begin(), low.end(), low.begin(), ::tolower);
    auto pos = low.find("error:");
    if (pos != std::string::npos) s = s.substr(pos + 6);
    // Trim
    auto start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return s;
    auto end   = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

// "OK (N rows, Tms, pool: X/Y in use)"
static inline std::string formatOk(int rowCount, long durationMs,
                                    int poolInUse, int poolTotal) {
    std::ostringstream oss;
    oss << "OK (" << rowCount << " rows, "
        << durationMs << "ms, pool: "
        << poolInUse << "/" << poolTotal << " in use)";
    return oss.str();
}

// "ERR reason"
static inline std::string formatErr(const std::string& reason) {
    std::string cleaned = stripErrorPrefix(reason);
    if (cleaned.empty()) cleaned = "request failed";
    return "ERR " + cleaned;
}

// Pipe-delimited SELECT table:
//   col1 | col2
//   -----+-----
//    v1  |  v2
//   (N rows, Tms, pool: X/Y in use)
static inline std::string formatSelect(const QueryResult& qr,
                                        int poolInUse, int poolTotal) {
    if (qr.fields.empty())
        return formatOk(qr.rowCount, qr.durationMs, poolInUse, poolTotal);

    int ncols = (int)qr.fields.size();

    // Compute column widths
    std::vector<size_t> widths(ncols);
    for (int c = 0; c < ncols; c++)
        widths[c] = qr.fields[c].size();
    for (const auto& row : qr.rows)
        for (int c = 0; c < ncols; c++)
            widths[c] = std::max(widths[c], row[c].size());

    auto padRight = [](const std::string& s, size_t w) {
        return s + std::string(w > s.size() ? w - s.size() : 0, ' ');
    };

    std::ostringstream oss;

    // Header row
    for (int c = 0; c < ncols; c++) {
        if (c) oss << " | ";
        oss << padRight(qr.fields[c], widths[c]);
    }
    oss << "\n";

    // Separator
    for (int c = 0; c < ncols; c++) {
        if (c) oss << "-+-";
        oss << std::string(widths[c], '-');
    }
    oss << "\n";

    // Data rows
    for (const auto& row : qr.rows) {
        for (int c = 0; c < ncols; c++) {
            if (c) oss << " | ";
            oss << padRight(row[c], widths[c]);
        }
        oss << "\n";
    }

    // Footer
    oss << "(" << qr.rowCount << " rows, "
        << qr.durationMs << "ms, pool: "
        << poolInUse << "/" << poolTotal << " in use)";

    return oss.str();
}
