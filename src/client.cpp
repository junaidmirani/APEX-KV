#include <apex/apex.hpp>

// ─────────────────────────────────────────────────────────────────────────────
class KVClient {
public:
    explicit KVClient(const std::string& host, uint16_t port) {
        fd_ = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (fd_ < 0) throw std::runtime_error("socket: " + std::string(strerror(errno)));
        int one = 1;
        setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
        addr.sin_port = htons(port);
        if (connect(fd_, (sockaddr*)&addr, sizeof addr) < 0)
            throw std::runtime_error("connect to " + host + ":" + std::to_string(port)
                                     + " — " + strerror(errno));
    }

    ~KVClient() { if (fd_ >= 0) close(fd_); }

    KVClient(const KVClient&)            = delete;
    KVClient& operator=(const KVClient&) = delete;

    std::string ping()                              { return rpc(Cmd::PING, {}, {}); }
    std::string get (const std::string& k)          { return rpc(Cmd::GET,  k,  {}); }
    std::string put (const std::string& k, const std::string& v) { return rpc(Cmd::PUT, k, v); }
    std::string del (const std::string& k)          { return rpc(Cmd::DEL,  k,  {}); }

    void repl() {
        std::cout <<
            "\n  ┌────────────────────────────────────────────────────┐\n"
            "  │  APEX-KV  Interactive Client                        │\n"
            "  │  GET <key>   PUT <key> <value>   DEL <key>  PING   │\n"
            "  │  METRICS     QUIT                                   │\n"
            "  └────────────────────────────────────────────────────┘\n\n";

        std::string line;
        while (true) {
            std::cout << "apex> " << std::flush;
            if (!std::getline(std::cin, line)) break;
            if (line.empty()) continue;

            std::istringstream ss(line);
            std::string cmd; ss >> cmd;
            for (auto& c : cmd) c = (char)std::toupper(c);

            if (cmd == "QUIT" || cmd == "Q" || cmd == "EXIT") break;

            if (cmd == "PING") {
                std::cout << ping() << "\n";
            } else if (cmd == "GET") {
                std::string k; ss >> k;
                if (k.empty()) { std::cerr << "Usage: GET <key>\n"; continue; }
                std::cout << get(k) << "\n";
            } else if (cmd == "PUT") {
                std::string k; ss >> k;
                std::string v; std::getline(ss >> std::ws, v);
                if (k.empty() || v.empty()) { std::cerr << "Usage: PUT <key> <value>\n"; continue; }
                std::cout << put(k, v) << "\n";
            } else if (cmd == "DEL") {
                std::string k; ss >> k;
                if (k.empty()) { std::cerr << "Usage: DEL <key>\n"; continue; }
                std::cout << del(k) << "\n";
            } else if (cmd == "METRICS") {
                // Raw metrics request
                ByteWriter w;
                w.header(Cmd::METRICS, 0, ++seq_);
                w.finalize();
                send_all(w.buf);
                auto [cmd_r, payload] = recv_response();
                if (cmd_r == Cmd::METRICS_RESP && payload.size() >= 32) {
                    ByteReader r(payload.data(), payload.size());
                    std::cout << "  ops_get=" << r.u64()
                              << "  ops_put=" << r.u64()
                              << "  p99_get=" << r.u64() << "µs"
                              << "  p99_put=" << r.u64() << "µs\n";
                }
            } else {
                std::cerr << "Unknown: " << cmd << "\n";
            }
        }
    }

private:
    int      fd_  = -1;
    uint32_t seq_ = 0;

    std::string rpc(Cmd cmd, const std::string& key, const std::string& val) {
        ByteWriter w;
        w.header(cmd, 0, ++seq_);
        if (!key.empty()) w.str(key);
        if (!val.empty()) w.str(val);
        w.finalize();

        if (!send_all(w.buf)) return "(send error)";

        auto [resp_cmd, payload] = recv_response();
        switch (resp_cmd) {
            case Cmd::PONG:      return "PONG";
            case Cmd::OK:        return "OK";
            case Cmd::NOT_FOUND: return "(nil)";
            case Cmd::REDIRECT:  {
                ByteReader r(payload.data(), payload.size());
                return "(redirect) leader at " + r.str();
            }
            case Cmd::ERROR: {
                if (!payload.empty()) {
                    ByteReader r(payload.data(), payload.size());
                    return "(error) " + r.str();
                }
                return "(error)";
            }
            case Cmd::VALUE: {
                ByteReader r(payload.data(), payload.size());
                return r.str();
            }
            default: return "(unknown response)";
        }
    }

public:
    bool send_all(const std::vector<uint8_t>& buf) {
        ssize_t sent = 0, total = (ssize_t)buf.size();
        while (sent < total) {
            ssize_t n = send(fd_, buf.data() + sent, (size_t)(total - sent), MSG_NOSIGNAL);
            if (n <= 0) return false;
            sent += n;
        }
        return true;
    }

    std::pair<Cmd, std::vector<uint8_t>> recv_response() {
        MsgHeader hdr{};
        if (!recv_exact((uint8_t*)&hdr, sizeof hdr)) return {Cmd::ERROR, {}};
        if (ntohl(hdr.magic) != WIRE_MAGIC)           return {Cmd::ERROR, {}};

        uint32_t plen = ntohl(hdr.payload_len);
        std::vector<uint8_t> payload(plen);
        if (plen > 0 && !recv_exact(payload.data(), plen)) return {Cmd::ERROR, {}};
        return {static_cast<Cmd>(hdr.cmd), std::move(payload)};
    }

    bool recv_exact(uint8_t* buf, std::size_t n) {
        std::size_t got = 0;
        while (got < n) {
            ssize_t r = recv(fd_, buf + got, n - got, 0);
            if (r <= 0) return false;
            got += (std::size_t)r;
        }
        return true;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// §19  BENCHMARK