// tenant-cli.cpp — Interactive CLI client for the multi-tenant proxy
//
// Build:
//   g++ -std=c++17 -O2 -o tenant-cli tenant-cli.cpp
//
// Usage:
//   ./tenant-cli --host localhost --port 6000 --tenant acme --api-key acme_k3y

#include <iostream>
#include <string>
#include <sstream>
#include <cstring>
#include <regex>

// POSIX
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>

// ============================================================
// Arg parsing
// ============================================================
struct Args {
    std::string host;
    int         port   = 0;
    std::string tenant;
    std::string apiKey;
};

static void usage() {
    std::cerr <<
        "Usage: ./tenant-cli\n"
        "  --host <host>\n"
        "  --port <port>\n"
        "  --tenant <tenant_id>\n"
        "  --api-key <api_key>\n";
    std::exit(1);
}

static Args parseArgs(int argc, char* argv[]) {
    Args a;
    for (int i = 1; i < argc - 1; i += 2) {
        std::string key = argv[i];
        std::string val = argv[i + 1];
        if      (key == "--host")    a.host   = val;
        else if (key == "--port")    a.port   = std::stoi(val);
        else if (key == "--tenant")  a.tenant = val;
        else if (key == "--api-key") a.apiKey = val;
    }
    if (a.host.empty() || a.port == 0 || a.tenant.empty() || a.apiKey.empty())
        usage();
    return a;
}

// ============================================================
// TCP connection helper
// ============================================================
static int connectToProxy(const std::string& host, int port) {
    addrinfo hints{}, *res = nullptr;
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    std::string portStr = std::to_string(port);
    if (getaddrinfo(host.c_str(), portStr.c_str(), &hints, &res) != 0) {
        std::cerr << "Cannot resolve host: " << host << "\n";
        std::exit(1);
    }

    int fd = -1;
    for (addrinfo* p = res; p; p = p->ai_next) {
        fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0) continue;
        if (connect(fd, p->ai_addr, p->ai_addrlen) == 0) break;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);

    if (fd < 0) {
        std::cerr << "Cannot connect to " << host << ":" << port << "\n";
        std::exit(1);
    }
    return fd;
}

// ============================================================
// Send a line (appends \n)
// ============================================================
static void sendLine(int fd, const std::string& line) {
    std::string msg = line + "\n";
    const char* buf = msg.c_str();
    size_t rem = msg.size();
    while (rem > 0) {
        ssize_t n = write(fd, buf, rem);
        if (n <= 0) { std::cerr << "Write error\n"; std::exit(1); }
        buf += n; rem -= n;
    }
}

// ============================================================
// Read response from proxy
//
// A response ends when we receive a "terminal" line:
//   - starts with OK or ERR
//   - matches footer pattern  "(N rows, Tms, ...)"
//   - starts with "last query:" (end of \stats block)
// ============================================================
static const std::regex RE_FOOTER  (R"(^\(\d+ rows?,)");
static const std::regex RE_OK_ERR  (R"(^(OK|ERR)\b)");

static std::string readResponse(int fd) {
    static std::string incoming;   // buffer surviving across calls
    std::string response;

    while (true) {
        // Try to consume a line from the buffer first
        auto pos = incoming.find('\n');
        if (pos == std::string::npos) {
            // Need more data
            char buf[4096];
            ssize_t n = read(fd, buf, sizeof(buf));
            if (n <= 0) return response;  // disconnected
            incoming.append(buf, n);
            continue;
        }

        std::string line = incoming.substr(0, pos);
        incoming.erase(0, pos + 1);
        if (!line.empty() && line.back() == '\r') line.pop_back();

        if (!response.empty()) response += "\n";
        response += line;

        // Check for terminal line
        bool done = std::regex_search(line, RE_OK_ERR)
                 || std::regex_search(line, RE_FOOTER)
                 || (line.rfind("last query:", 0) == 0);
        if (done) break;
    }
    return response;
}

// ============================================================
// Main
// ============================================================
int main(int argc, char* argv[]) {
    Args args = parseArgs(argc, argv);

    int fd = connectToProxy(args.host, args.port);

    // Handshake
    sendLine(fd, "HELLO " + args.tenant + " " + args.apiKey);
    std::string helloResp = readResponse(fd);

    if (helloResp.substr(0, 3) != "OK ") {
        std::cerr << "Authentication failed: " << helloResp << "\n";
        close(fd);
        return 1;
    }

    std::string sessionId = helloResp.substr(3);
    std::cout << "connected as tenant '" << args.tenant
              << "' (session id " << sessionId << ")\n";

    // REPL — read from stdin, send to proxy, print response
    std::string prompt = args.tenant + "> ";
    std::cout << prompt << std::flush;

    std::string inputLine;
    while (std::getline(std::cin, inputLine)) {
        // Trim
        auto s = inputLine.find_first_not_of(" \t\r\n");
        if (s == std::string::npos) { std::cout << prompt << std::flush; continue; }
        auto e = inputLine.find_last_not_of(" \t\r\n");
        inputLine = inputLine.substr(s, e - s + 1);

        if (inputLine.empty()) { std::cout << prompt << std::flush; continue; }

        // Quit
        if (inputLine == "quit" || inputLine == "exit" || inputLine == "QUIT") {
            sendLine(fd, "QUIT");
            readResponse(fd);
            break;
        }

        // \burst N — handle client-side per spec
        if (inputLine.size() > 7 && inputLine.substr(0, 7) == "\\burst ") {
            int n = 0;
            try { n = std::stoi(inputLine.substr(7)); } catch(...) {}
            if (n <= 0) {
                std::cout << "ERR usage: \\burst <N>\n" << prompt << std::flush;
                continue;
            }
            int success = 0;
            int rejected = 0;
            std::string lastErr;
            for (int i = 0; i < n; i++) {
                sendLine(fd, "QUERY SELECT 1");
                std::string resp = readResponse(fd);
                if (resp.find("ERR") == 0) {
                    rejected++;
                    if (lastErr.empty()) lastErr = resp;
                } else {
                    success++;
                }
            }
            if (success > 0) std::cout << "sent " << success << " queries successfully...\n";
            if (rejected > 0) {
                if (success > 0) std::cout << "sent " << rejected << " more queries...\n";
                std::cout << lastErr << "\n";
            }
            std::cout << prompt << std::flush;
            continue;
        }

        // \stats, \tables — send as-is
        if (!inputLine.empty() && inputLine[0] == '\\') {
            sendLine(fd, inputLine);
            std::cout << readResponse(fd) << "\n";
            std::cout << prompt << std::flush;
            continue;
        }

        // Everything else: wrap in QUERY
        sendLine(fd, "QUERY " + inputLine);
        std::cout << readResponse(fd) << "\n";
        std::cout << prompt << std::flush;
    }

    close(fd);
    return 0;
}
