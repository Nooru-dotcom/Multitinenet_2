#pragma once
// backendcfg.hpp — BackendConfig POD struct, shared by pool.hpp and backend.hpp
// Kept in its own header so pool.hpp doesn't need to include the full backend.hpp

#include <string>

struct BackendConfig {
    std::string host;
    int         port;
    std::string user;
    std::string password;
    std::string database;
};
