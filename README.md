# Multitenant Database Proxy

A robust, multi-tenant database proxy server written in C++ that enforces strict tenant isolation, connection pooling, and rate limiting for multi-tenant architectures.

## Features

- **Tenant Authentication & Configuration:** Loads tenant configurations dynamically from `tenants.conf`, supporting individual API keys, rate limits, and maximum connection limits per tenant.
- **Strict Query Filtering & Security:** Implements a query denylist / rewriter to enforce tenant isolation. It prevents explicit cross-schema references and rejects unauthorized `SET search_path` and `SET ROLE` commands.
- **Token-Bucket Rate Limiting:** Enforces strict per-tenant queries-per-second (QPS) using a highly efficient token-bucket algorithm, allowing for temporary bursts while strictly managing overall query volume.
- **Connection Management & Pooling:** Manages concurrent connections per tenant to prevent resource starvation, ensuring no tenant can exceed their configured `max_connections`.
- **Metrics Collection:** Tracks system and per-tenant performance metrics for monitoring.

## Configuration (`tenants.conf`)

The server configuration relies on `tenants.conf`. Each line specifies the configuration for a tenant in the following format:
`tenant_id:api_key:rate_limit_per_sec:max_connections`

Example:
```
tenant_a:secretkey123:50:10
tenant_b:anotherkey456:100:20
```

## Building the Project

The project is built using `make`.

```bash
# Compile the proxy server and CLI
make

# Run the unit tests
make test
```

## Project Structure

- `src/tenant-proxy.cpp`: The main entry point for the proxy server.
- `src/tenant-cli.cpp`: A command-line interface tool for interacting with the proxy.
- `src/config.hpp`: Parses and validates tenant configurations.
- `src/sessions.hpp`: Manages active sessions and implements the token-bucket rate limiter.
- `src/rewriter.hpp`: Ensures query safety by checking against a denylist.
- `src/pool.hpp` & `src/backend.hpp`: Manages backend database connection pooling.
- `tests/unit_test.cpp`: Contains the unit tests for core functionality.
