extern "C" {
#include "php.h"
#include "ext/standard/info.h"
#include "Zend/zend_API.h"
#include "Zend/zend_interfaces.h"
#include "Zend/zend_exceptions.h"
}

#include "php_kislayphp_queue.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <map>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <atomic>
#include <mutex>
#include <thread>
#include <deque>

#include "Zend/zend_smart_str.h"
#include "ext/standard/php_var.h"

#ifndef zend_call_method_with_0_params
static inline void kislayphp_call_method_with_0_params(
    zend_object *obj,
    zend_class_entry *obj_ce,
    zend_function **fn_proxy,
    const char *function_name,
    zval *retval) {
    zend_call_method(obj, obj_ce, fn_proxy, function_name, std::strlen(function_name), retval, 0, nullptr, nullptr);
}
#define zend_call_method_with_0_params(obj, obj_ce, fn_proxy, function_name, retval) \
    kislayphp_call_method_with_0_params(obj, obj_ce, fn_proxy, function_name, retval)
#endif

#ifndef zend_call_method_with_1_params
static inline void kislayphp_call_method_with_1_params(
    zend_object *obj,
    zend_class_entry *obj_ce,
    zend_function **fn_proxy,
    const char *function_name,
    zval *retval,
    zval *param1) {
    zend_call_method(obj, obj_ce, fn_proxy, function_name, std::strlen(function_name), retval, 1, param1, nullptr);
}
#define zend_call_method_with_1_params(obj, obj_ce, fn_proxy, function_name, retval, param1) \
    kislayphp_call_method_with_1_params(obj, obj_ce, fn_proxy, function_name, retval, param1)
#endif

#ifndef zend_call_method_with_2_params
static inline void kislayphp_call_method_with_2_params(
    zend_object *obj,
    zend_class_entry *obj_ce,
    zend_function **fn_proxy,
    const char *function_name,
    zval *retval,
    zval *param1,
    zval *param2) {
    zend_call_method(obj, obj_ce, fn_proxy, function_name, std::strlen(function_name), retval, 2, param1, param2);
}
#define zend_call_method_with_2_params(obj, obj_ce, fn_proxy, function_name, retval, param1, param2) \
    kislayphp_call_method_with_2_params(obj, obj_ce, fn_proxy, function_name, retval, param1, param2)
#endif

static zend_class_entry *kislayphp_queue_ce;
static zend_class_entry *kislayphp_queue_client_interface_ce;
static zend_class_entry *kislayphp_queue_server_ce;
static zend_class_entry *kislayphp_queue_client_ce;
static zend_class_entry *kislayphp_queue_worker_ce;
static zend_class_entry *kislayphp_queue_job_ce;

static zend_object_handlers kislayphp_queue_handlers;
static zend_object_handlers kislayphp_queue_server_handlers;
static zend_object_handlers kislayphp_queue_client_handlers;
static zend_object_handlers kislayphp_queue_worker_handlers;
static zend_object_handlers kislayphp_queue_job_handlers;

struct kislay_http_request_t {
    std::string method;
    std::string uri;
    std::string path;
    std::string query;
    std::string body;
    std::map<std::string, std::string> headers;
};

struct kislay_http_url_t {
    std::string host;
    int port;
    std::string path;
};

struct kislay_queue_config_t {
    zend_long visibility_timeout_ms;
    zend_long max_attempts;
    zend_long retry_backoff_ms;
    std::string dead_letter_queue;

    kislay_queue_config_t()
        : visibility_timeout_ms(30000),
          max_attempts(3),
          retry_backoff_ms(1000),
          dead_letter_queue() {}
};

struct kislay_queue_stats_t {
    std::uint64_t pushed_total;
    std::uint64_t fetched_total;
    std::uint64_t acked_total;
    std::uint64_t nacked_total;
    std::uint64_t retried_total;
    std::uint64_t dead_lettered_total;
    std::uint64_t purged_total;

    kislay_queue_stats_t()
        : pushed_total(0),
          fetched_total(0),
          acked_total(0),
          nacked_total(0),
          retried_total(0),
          dead_lettered_total(0),
          purged_total(0) {}
};

struct kislay_queue_job_record_t {
    std::string id;
    std::string queue;
    std::string payload_bytes;
    std::string headers_bytes;
    zend_long attempts;
    zend_long max_attempts;
    std::uint64_t created_at_ms;
    std::uint64_t available_at_ms;
    std::uint64_t lease_until_ms;
    std::string status;
    std::string worker_id;

    kislay_queue_job_record_t()
        : attempts(0),
          max_attempts(3),
          created_at_ms(0),
          available_at_ms(0),
          lease_until_ms(0),
          status("ready") {}
};

struct kislay_queue_fetch_result_t {
    bool found;
    std::string id;
    std::string queue;
    std::string payload_bytes;
    std::string headers_bytes;
    zend_long attempts;
    zend_long max_attempts;
    std::uint64_t available_at_ms;

    kislay_queue_fetch_result_t()
        : found(false), attempts(0), max_attempts(0), available_at_ms(0) {}
};

typedef struct _php_kislayphp_queue_t {
    std::unordered_map<std::string, std::vector<zval>> queues;
    pthread_mutex_t lock;
    zval client;
    bool has_client;
    zend_object std;
} php_kislayphp_queue_t;

typedef struct _php_kislayphp_queue_server_t {
    std::unordered_map<std::string, kislay_queue_config_t> configs;
    std::unordered_map<std::string, std::vector<std::string>> queue_jobs;
    std::unordered_map<std::string, kislay_queue_job_record_t> jobs;
    std::unordered_map<std::string, kislay_queue_stats_t> stats;
    std::string host;
    zend_long port;
    int listen_fd;
    bool running;
    std::uint64_t next_job_id;
    pthread_mutex_t lock;
    zend_object std;
} php_kislayphp_queue_server_t;

typedef struct _php_kislayphp_queue_client_t {
    std::string base_url;
    zend_object std;
} php_kislayphp_queue_client_t;

typedef struct _php_kislayphp_queue_worker_t {
    std::string base_url;
    std::atomic<bool> stop_requested{false};  // atomic: stop() called from another thread
    zend_object std;
} php_kislayphp_queue_worker_t;

typedef struct _php_kislayphp_queue_job_t {
    std::string base_url;
    std::string id;
    std::string queue;
    zval payload;
    zval headers;
    zend_long attempts;
    zend_long max_attempts;
    std::uint64_t available_at_ms;
    bool finished;
    zend_object std;
} php_kislayphp_queue_job_t;

struct kislay_scoped_lock_t {
    pthread_mutex_t *lock;

    explicit kislay_scoped_lock_t(pthread_mutex_t *target) : lock(target) {
        pthread_mutex_lock(lock);
    }

    ~kislay_scoped_lock_t() {
        pthread_mutex_unlock(lock);
    }
};

// Server::run()'s accept loop spawns a brand-new std::thread per client
// connection (see the handler below), and every non-trivial request branch
// touches the Zend engine directly - kislay_json_decode_assoc() calls into
// call_user_function()/json_decode(), and array_init()/add_assoc_*()/
// zval_ptr_dtor() all touch zend_mm. On an NTS build there is no per-thread
// engine isolation, so two client threads doing this concurrently (trivially
// reachable: any two overlapping requests, e.g. a producer push racing a
// worker's fetch) race on the same executor/allocator state with no
// happens-before edge - the same zend_mm heap corruption class found and
// fixed in kislayphp/socket's civetweb callback path this session. This
// mutex serializes every Zend touch across all connection threads, mirroring
// that fix (kislay_socket_php_call_lock in socket/kislay_socket.cpp).
// Recursive because some locked helpers (e.g. request handling that also
// takes obj->lock for the queue data structures) call back into Zend-facing
// code under it; lock ordering here is fixed as php_call_lock outermost,
// obj->lock nested inside, never the reverse.
static std::recursive_mutex kislay_queue_php_call_lock;

static inline php_kislayphp_queue_t *php_kislayphp_queue_from_obj(zend_object *obj) {
    return reinterpret_cast<php_kislayphp_queue_t *>(reinterpret_cast<char *>(obj) - XtOffsetOf(php_kislayphp_queue_t, std));
}

static inline php_kislayphp_queue_server_t *php_kislayphp_queue_server_from_obj(zend_object *obj) {
    return reinterpret_cast<php_kislayphp_queue_server_t *>(reinterpret_cast<char *>(obj) - XtOffsetOf(php_kislayphp_queue_server_t, std));
}

static inline php_kislayphp_queue_client_t *php_kislayphp_queue_client_from_obj(zend_object *obj) {
    return reinterpret_cast<php_kislayphp_queue_client_t *>(reinterpret_cast<char *>(obj) - XtOffsetOf(php_kislayphp_queue_client_t, std));
}

static inline php_kislayphp_queue_worker_t *php_kislayphp_queue_worker_from_obj(zend_object *obj) {
    return reinterpret_cast<php_kislayphp_queue_worker_t *>(reinterpret_cast<char *>(obj) - XtOffsetOf(php_kislayphp_queue_worker_t, std));
}

static inline php_kislayphp_queue_job_t *php_kislayphp_queue_job_from_obj(zend_object *obj) {
    return reinterpret_cast<php_kislayphp_queue_job_t *>(reinterpret_cast<char *>(obj) - XtOffsetOf(php_kislayphp_queue_job_t, std));
}

static std::uint64_t kislay_now_ms() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

static void kislay_sleep_ms(zend_long value) {
    if (value <= 0) {
        value = 1;
    }
    usleep(static_cast<useconds_t>(value * 1000));
}

static bool kislay_call_function(const char *name, zval *retval, uint32_t param_count, zval params[]) {
    zval function_name;
    ZVAL_STRING(&function_name, name);
    int result = call_user_function(EG(function_table), nullptr, &function_name, retval, param_count, params);
    zval_ptr_dtor(&function_name);
    return result == SUCCESS;
}

static bool kislay_json_encode_zval(zval *value, std::string *out) {
    zval retval;
    zval params[1];
    ZVAL_COPY(&params[0], value);
    ZVAL_UNDEF(&retval);
    bool ok = kislay_call_function("json_encode", &retval, 1, params);
    zval_ptr_dtor(&params[0]);
    if (!ok || Z_TYPE(retval) != IS_STRING) {
        if (!Z_ISUNDEF(retval)) {
            zval_ptr_dtor(&retval);
        }
        return false;
    }
    *out = std::string(Z_STRVAL(retval), Z_STRLEN(retval));
    zval_ptr_dtor(&retval);
    return true;
}

static bool kislay_json_decode_assoc(const std::string &json, zval *return_value) {
    zval retval;
    zval params[2];
    ZVAL_STRINGL(&params[0], json.c_str(), json.size());
    ZVAL_TRUE(&params[1]);
    ZVAL_UNDEF(&retval);
    bool ok = kislay_call_function("json_decode", &retval, 2, params);
    zval_ptr_dtor(&params[0]);
    zval_ptr_dtor(&params[1]);
    if (!ok || Z_ISUNDEF(retval)) {
        if (!Z_ISUNDEF(retval)) {
            zval_ptr_dtor(&retval);
        }
        return false;
    }
    ZVAL_COPY_VALUE(return_value, &retval);
    return true;
}

static std::string kislay_trim(const std::string &value) {
    std::size_t start = 0;
    while (start < value.size() && std::isspace(static_cast<unsigned char>(value[start]))) {
        start++;
    }
    std::size_t end = value.size();
    while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1]))) {
        end--;
    }
    return value.substr(start, end - start);
}

static std::string kislay_to_lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return value;
}

static std::string kislay_url_decode(const std::string &value) {
    std::string result;
    result.reserve(value.size());
    for (std::size_t i = 0; i < value.size(); ++i) {
        if (value[i] == '%' && i + 2 < value.size()) {
            char hex[3] = {value[i + 1], value[i + 2], '\0'};
            char *endptr = nullptr;
            long decoded = std::strtol(hex, &endptr, 16);
            if (endptr != nullptr && *endptr == '\0') {
                result.push_back(static_cast<char>(decoded));
                i += 2;
                continue;
            }
        }
        if (value[i] == '+') {
            result.push_back(' ');
        } else {
            result.push_back(value[i]);
        }
    }
    return result;
}

static std::vector<std::string> kislay_split(const std::string &value, char delimiter) {
    std::vector<std::string> parts;
    std::stringstream stream(value);
    std::string item;
    while (std::getline(stream, item, delimiter)) {
        parts.push_back(item);
    }
    return parts;
}

static bool kislay_parse_http_url(const std::string &url, kislay_http_url_t *parsed) {
    std::string work = url;
    const std::string http_prefix("http://");
    if (work.compare(0, http_prefix.size(), http_prefix) == 0) {
        work = work.substr(http_prefix.size());
    }
    std::size_t slash = work.find('/');
    std::string authority = slash == std::string::npos ? work : work.substr(0, slash);
    parsed->path = slash == std::string::npos ? "/" : work.substr(slash);
    std::size_t colon = authority.rfind(':');
    if (colon == std::string::npos) {
        parsed->host = authority;
        parsed->port = 80;
    } else {
        parsed->host = authority.substr(0, colon);
        parsed->port = std::atoi(authority.substr(colon + 1).c_str());
        if (parsed->port <= 0) {
            parsed->port = 80;
        }
    }
    return !parsed->host.empty();
}


// ─────────────────────────────────────────────────────────────────────────────
// HTTP connection pool — reuse TCP sockets across requests to the same host:port
// instead of creating a new socket per operation (the biggest perf gap).
// Pool is thread_local to avoid locking; each worker thread has its own pool.
// ─────────────────────────────────────────────────────────────────────────────
struct KislayPooledSocket {
    int fd;
    std::chrono::steady_clock::time_point last_used;
};

class KislayHttpPool {
public:
    static KislayHttpPool& get() {
        static thread_local KislayHttpPool instance;
        return instance;
    }

    ~KislayHttpPool() { clear(); }

    // Acquire an existing connected socket or return -1
    int acquire(const std::string &host, int port) {
        std::string key = host + ":" + std::to_string(port);
        auto it = pool_.find(key);
        if (it == pool_.end() || it->second.empty()) return -1;

        auto &q = it->second;
        while (!q.empty()) {
            auto ps = std::move(q.back());
            q.pop_back();
            // Idle timeout: 30 seconds
            auto age = std::chrono::steady_clock::now() - ps.last_used;
            if (std::chrono::duration_cast<std::chrono::seconds>(age).count() > 30) {
                close(ps.fd);
                continue;
            }
            // Quick liveness check with MSG_PEEK
            char buf;
            ssize_t r = recv(ps.fd, &buf, 1, MSG_PEEK | MSG_DONTWAIT);
            if (r == 0 || (r < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
                close(ps.fd);
                continue;
            }
            return ps.fd;
        }
        return -1;
    }

    // Return a socket to the pool after successful use (keep-alive)
    void release(int fd, const std::string &host, int port) {
        if (fd < 0) return;
        std::string key = host + ":" + std::to_string(port);
        auto &q = pool_[key];
        if (static_cast<int>(q.size()) >= MAX_PER_HOST) {
            close(q.front().fd);
            q.pop_front();
        }
        q.push_back({fd, std::chrono::steady_clock::now()});
    }

    void clear() {
        for (auto &kv : pool_)
            for (auto &ps : kv.second)
                close(ps.fd);
        pool_.clear();
    }

private:
    static constexpr int MAX_PER_HOST = 8;
    std::unordered_map<std::string, std::deque<KislayPooledSocket>> pool_;
};

static int kislay_connect(const std::string &host, int port, std::string *error) {
    struct addrinfo hints;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo *result = nullptr;
    std::string port_str = std::to_string(port);
    if (getaddrinfo(host.c_str(), port_str.c_str(), &hints, &result) != 0) {
        if (error) *error = "getaddrinfo failed for " + host;
        return -1;
    }

    int fd = -1;
    for (struct addrinfo *rp = result; rp != nullptr; rp = rp->ai_next) {
        fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd == -1) continue;
        // TCP_NODELAY: disable Nagle for low-latency queue operations
        int flag = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
        if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) break;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(result);

    if (fd == -1 && error) *error = "connect failed to " + host + ":" + port_str;
    return fd;
}

static bool kislay_http_request(const std::string &method, const std::string &url, const std::string &body, int *status_code, std::string *response_body, std::string *error) {
    kislay_http_url_t parsed;
    if (!kislay_parse_http_url(url, &parsed)) {
        if (error != nullptr) {
            *error = "Invalid URL";
        }
        return false;
    }

    // Try to acquire a pooled socket first
    bool reused = false;
    int fd = KislayHttpPool::get().acquire(parsed.host, parsed.port);
    if (fd < 0) {
        std::string conn_err;
        fd = kislay_connect(parsed.host, parsed.port, &conn_err);
        if (fd < 0) {
            if (error) *error = conn_err;
            return false;
        }
    } else {
        reused = true;
    }

    auto do_request = [&](int sock) -> bool {
        std::ostringstream request;
        request << method << " " << parsed.path << " HTTP/1.1\r\n";
        request << "Host: " << parsed.host << "\r\n";
        request << "Connection: keep-alive\r\n";
        if (!body.empty()) {
            request << "Content-Type: application/json\r\n";
            request << "Content-Length: " << body.size() << "\r\n";
        }
        request << "\r\n";
        request << body;

        const std::string wire = request.str();
        std::size_t sent_bytes = 0;
        while (sent_bytes < wire.size()) {
            ssize_t wrote = send(sock, wire.data() + sent_bytes, wire.size() - sent_bytes, MSG_NOSIGNAL);
            if (wrote <= 0) {
                if (error) *error = "send failed";
                return false;
            }
            sent_bytes += static_cast<std::size_t>(wrote);
        }

        // Read until we have Content-Length bytes (or connection close)
        std::string response;
        char buffer[8192];
        std::size_t header_end_pos = std::string::npos;
        long long content_len = -1;
        for (;;) {
            ssize_t received = recv(sock, buffer, sizeof(buffer), 0);
            if (received == 0) break;
            if (received < 0) {
                if (error) *error = "recv failed";
                return false;
            }
            response.append(buffer, static_cast<std::size_t>(received));
            if (header_end_pos == std::string::npos) {
                header_end_pos = response.find("\r\n\r\n");
                if (header_end_pos != std::string::npos) {
                    // Parse Content-Length
                    std::string hdrs = response.substr(0, header_end_pos);
                    auto cl_pos = hdrs.find("Content-Length:");
                    if (cl_pos == std::string::npos) cl_pos = hdrs.find("content-length:");
                    if (cl_pos != std::string::npos) {
                        content_len = std::atoll(hdrs.c_str() + cl_pos + 15);
                    }
                }
            }
            if (header_end_pos != std::string::npos && content_len >= 0) {
                std::size_t body_start = header_end_pos + 4;
                if (response.size() >= body_start + static_cast<std::size_t>(content_len)) break;
            }
        }

        if (header_end_pos == std::string::npos) {
            if (error) *error = "Invalid HTTP response";
            return false;
        }

        std::string hdrs_part = response.substr(0, header_end_pos);
        if (response_body) *response_body = response.substr(header_end_pos + 4);

        std::size_t fs = hdrs_part.find(' ');
        if (fs == std::string::npos) { if (error) *error = "Invalid status line"; return false; }
        std::size_t ss = hdrs_part.find(' ', fs + 1);
        std::string code_str = hdrs_part.substr(fs + 1, ss == std::string::npos ? std::string::npos : (ss - fs - 1));
        if (status_code) *status_code = std::atoi(code_str.c_str());
        return true;
    };

    bool ok = do_request(fd);
    if (!ok && reused) {
        // Stale pooled socket — reconnect once
        close(fd);
        std::string conn_err;
        fd = kislay_connect(parsed.host, parsed.port, &conn_err);
        if (fd < 0) { if (error) *error = conn_err; return false; }
        ok = do_request(fd);
    }
    if (!ok) { close(fd); return false; }
    // Return socket to pool for reuse
    KislayHttpPool::get().release(fd, parsed.host, parsed.port);
    return true;
}

static bool kislay_http_read_request(int fd, kislay_http_request_t *request) {
    std::string incoming;
    char buffer[4096];
    std::size_t header_end = std::string::npos;
    while ((header_end = incoming.find("\r\n\r\n")) == std::string::npos) {
        ssize_t received = recv(fd, buffer, sizeof(buffer), 0);
        if (received <= 0) {
            return false;
        }
        incoming.append(buffer, static_cast<std::size_t>(received));
        if (incoming.size() > 1024 * 1024) {
            return false;
        }
    }

    std::string header_block = incoming.substr(0, header_end);
    std::string body = incoming.substr(header_end + 4);

    std::istringstream stream(header_block);
    std::string request_line;
    if (!std::getline(stream, request_line)) {
        return false;
    }
    if (!request_line.empty() && request_line[request_line.size() - 1] == '\r') {
        request_line.erase(request_line.size() - 1);
    }
    std::istringstream line_stream(request_line);
    line_stream >> request->method >> request->uri;

    std::string header_line;
    std::size_t content_length = 0;
    while (std::getline(stream, header_line)) {
        if (!header_line.empty() && header_line[header_line.size() - 1] == '\r') {
            header_line.erase(header_line.size() - 1);
        }
        std::size_t colon = header_line.find(':');
        if (colon == std::string::npos) {
            continue;
        }
        std::string key = kislay_to_lower(kislay_trim(header_line.substr(0, colon)));
        std::string value = kislay_trim(header_line.substr(colon + 1));
        request->headers[key] = value;
        if (key == "content-length") {
            content_length = static_cast<std::size_t>(std::strtoull(value.c_str(), nullptr, 10));
        }
    }

    while (body.size() < content_length) {
        ssize_t received = recv(fd, buffer, sizeof(buffer), 0);
        if (received <= 0) {
            return false;
        }
        body.append(buffer, static_cast<std::size_t>(received));
    }
    request->body = body.substr(0, content_length);

    std::size_t q = request->uri.find('?');
    request->path = q == std::string::npos ? request->uri : request->uri.substr(0, q);
    request->query = q == std::string::npos ? std::string() : request->uri.substr(q + 1);
    return true;
}

static void kislay_http_send_response(int fd, int status_code, const std::string &content_type, const std::string &body) {
    std::ostringstream response;
    const char *status_text = "OK";
    if (status_code == 201) status_text = "Created";
    if (status_code == 400) status_text = "Bad Request";
    if (status_code == 404) status_text = "Not Found";
    if (status_code == 405) status_text = "Method Not Allowed";
    if (status_code == 500) status_text = "Internal Server Error";
    response << "HTTP/1.1 " << status_code << ' ' << status_text << "\r\n";
    response << "Content-Type: " << content_type << "\r\n";
    response << "Content-Length: " << body.size() << "\r\n";
    response << "Connection: close\r\n\r\n";
    response << body;
    const std::string wire = response.str();
    send(fd, wire.data(), wire.size(), 0);
}

static std::string kislay_hex_encode(const std::string &input) {
    static const char *digits = "0123456789abcdef";
    std::string out;
    out.reserve(input.size() * 2);
    for (std::size_t i = 0; i < input.size(); ++i) {
        unsigned char ch = static_cast<unsigned char>(input[i]);
        out.push_back(digits[(ch >> 4) & 0x0F]);
        out.push_back(digits[ch & 0x0F]);
    }
    return out;
}

static int kislay_hex_value(char ch) {
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return 10 + (ch - 'a');
    if (ch >= 'A' && ch <= 'F') return 10 + (ch - 'A');
    return -1;
}

static bool kislay_hex_decode(const std::string &input, std::string *out) {
    if ((input.size() % 2) != 0) {
        return false;
    }
    out->clear();
    out->reserve(input.size() / 2);
    for (std::size_t i = 0; i < input.size(); i += 2) {
        int high = kislay_hex_value(input[i]);
        int low = kislay_hex_value(input[i + 1]);
        if (high < 0 || low < 0) {
            return false;
        }
        out->push_back(static_cast<char>((high << 4) | low));
    }
    return true;
}

static bool kislay_serialize_payload(zval *payload, std::string *out) {
    zval retval;
    zval params[1];
    ZVAL_COPY(&params[0], payload);
    ZVAL_UNDEF(&retval);
    bool ok = kislay_call_function("serialize", &retval, 1, params);
    zval_ptr_dtor(&params[0]);
    if (!ok || Z_TYPE(retval) != IS_STRING) {
        if (!Z_ISUNDEF(retval)) {
            zval_ptr_dtor(&retval);
        }
        return false;
    }
    out->assign(Z_STRVAL(retval), Z_STRLEN(retval));
    zval_ptr_dtor(&retval);
    return true;
}

static bool kislay_unserialize_payload(const std::string &data, zval *out) {
    zval retval;
    zval params[1];
    ZVAL_STRINGL(&params[0], data.c_str(), data.size());
    ZVAL_UNDEF(&retval);
    bool ok = kislay_call_function("unserialize", &retval, 1, params);
    zval_ptr_dtor(&params[0]);
    if (!ok || Z_ISUNDEF(retval)) {
        if (!Z_ISUNDEF(retval)) {
            zval_ptr_dtor(&retval);
        }
        return false;
    }
    ZVAL_COPY_VALUE(out, &retval);
    return true;
}

static bool kislay_hash_find_string(HashTable *ht, const char *key, std::string *out) {
    zval *value = zend_hash_str_find(ht, key, std::strlen(key));
    if (value == nullptr || Z_TYPE_P(value) == IS_NULL) {
        return false;
    }
    zend_string *str = zval_get_string(value);
    *out = std::string(ZSTR_VAL(str), ZSTR_LEN(str));
    zend_string_release(str);
    return true;
}

static zend_long kislay_hash_find_long(HashTable *ht, const char *key, zend_long fallback) {
    zval *value = zend_hash_str_find(ht, key, std::strlen(key));
    if (value == nullptr || Z_TYPE_P(value) == IS_NULL) {
        return fallback;
    }
    return zval_get_long(value);
}

static bool kislay_hash_find_bool(HashTable *ht, const char *key, bool fallback) {
    zval *value = zend_hash_str_find(ht, key, std::strlen(key));
    if (value == nullptr || Z_TYPE_P(value) == IS_NULL) {
        return fallback;
    }
    return zend_is_true(value);
}

static zval *kislay_hash_find(HashTable *ht, const char *key) {
    return zend_hash_str_find(ht, key, std::strlen(key));
}

static bool kislay_queue_is_dlq_name(const std::string &queue) {
    return queue.size() >= 4 && queue.compare(queue.size() - 4, 4, ".dlq") == 0;
}

static kislay_queue_config_t kislay_queue_default_config(const std::string &queue) {
    kislay_queue_config_t config;
    if (!kislay_queue_is_dlq_name(queue)) {
        config.dead_letter_queue = queue + ".dlq";
    }
    return config;
}

static std::string kislay_queue_make_job_id(php_kislayphp_queue_server_t *server) {
    server->next_job_id++;
    std::ostringstream oss;
    oss << "job_" << static_cast<unsigned long long>(kislay_now_ms()) << '_' << static_cast<unsigned long long>(server->next_job_id);
    return oss.str();
}

static kislay_queue_config_t &kislay_queue_ensure_config_locked(php_kislayphp_queue_server_t *server, const std::string &queue) {
    std::unordered_map<std::string, kislay_queue_config_t>::iterator it = server->configs.find(queue);
    if (it == server->configs.end()) {
        it = server->configs.insert(std::make_pair(queue, kislay_queue_default_config(queue))).first;
    }
    return it->second;
}

static kislay_queue_stats_t &kislay_queue_ensure_stats_locked(php_kislayphp_queue_server_t *server, const std::string &queue) {
    return server->stats[queue];
}

static void kislay_queue_remove_job_id(std::vector<std::string> *ids, const std::string &job_id) {
    ids->erase(std::remove(ids->begin(), ids->end(), job_id), ids->end());
}

static void kislay_queue_move_to_dlq_locked(php_kislayphp_queue_server_t *server, kislay_queue_job_record_t *job) {
    kislay_queue_config_t &source_config = kislay_queue_ensure_config_locked(server, job->queue);
    std::string source_queue = job->queue;
    kislay_queue_ensure_stats_locked(server, source_queue).dead_lettered_total++;

    if (source_config.dead_letter_queue.empty()) {
        std::vector<std::string> &source_ids = server->queue_jobs[source_queue];
        kislay_queue_remove_job_id(&source_ids, job->id);
        server->jobs.erase(job->id);
        return;
    }

    std::string dlq_name = source_config.dead_letter_queue;
    kislay_queue_ensure_config_locked(server, dlq_name);

    std::vector<std::string> &source_ids = server->queue_jobs[source_queue];
    kislay_queue_remove_job_id(&source_ids, job->id);

    job->queue = dlq_name;
    job->status = "ready";
    job->lease_until_ms = 0;
    job->available_at_ms = kislay_now_ms();
    job->worker_id.clear();

    server->queue_jobs[dlq_name].push_back(job->id);
}

static bool kislay_queue_job_ack_locked(php_kislayphp_queue_server_t *server, const std::string &job_id) {
    std::unordered_map<std::string, kislay_queue_job_record_t>::iterator it = server->jobs.find(job_id);
    if (it == server->jobs.end()) {
        return false;
    }
    std::string queue = it->second.queue;
    std::vector<std::string> &ids = server->queue_jobs[queue];
    kislay_queue_remove_job_id(&ids, job_id);
    kislay_queue_ensure_stats_locked(server, queue).acked_total++;
    server->jobs.erase(it);
    return true;
}

static bool kislay_queue_job_nack_locked(php_kislayphp_queue_server_t *server, const std::string &job_id, bool requeue, zend_long delay_ms) {
    std::unordered_map<std::string, kislay_queue_job_record_t>::iterator it = server->jobs.find(job_id);
    if (it == server->jobs.end()) {
        return false;
    }
    kislay_queue_job_record_t &job = it->second;
    kislay_queue_stats_t &stats = kislay_queue_ensure_stats_locked(server, job.queue);
    stats.nacked_total++;
    kislay_queue_config_t &config = kislay_queue_ensure_config_locked(server, job.queue);

    if (!requeue || job.attempts >= job.max_attempts) {
        kislay_queue_move_to_dlq_locked(server, &job);
        return true;
    }

    zend_long actual_delay = delay_ms >= 0 ? delay_ms : config.retry_backoff_ms;
    if (actual_delay < 0) {
        actual_delay = 0;
    }
    job.worker_id.clear();
    job.lease_until_ms = 0;
    job.available_at_ms = kislay_now_ms() + static_cast<std::uint64_t>(actual_delay);
    job.status = actual_delay > 0 ? "delayed" : "ready";
    stats.retried_total++;
    return true;
}

static void kislay_queue_reconcile_job_locked(php_kislayphp_queue_server_t *server, kislay_queue_job_record_t *job, std::uint64_t now_ms) {
    if (job->status == "delayed" && job->available_at_ms <= now_ms) {
        job->status = "ready";
        return;
    }

    if (job->status == "leased" && job->lease_until_ms <= now_ms) {
        kislay_queue_config_t &config = kislay_queue_ensure_config_locked(server, job->queue);
        if (job->attempts >= job->max_attempts) {
            kislay_queue_move_to_dlq_locked(server, job);
            return;
        }
        job->worker_id.clear();
        job->lease_until_ms = 0;
        job->available_at_ms = now_ms + static_cast<std::uint64_t>(config.retry_backoff_ms > 0 ? config.retry_backoff_ms : 0);
        job->status = config.retry_backoff_ms > 0 ? "delayed" : "ready";
        kislay_queue_ensure_stats_locked(server, job->queue).retried_total++;
    }
}

static std::string kislay_queue_push_locked(php_kislayphp_queue_server_t *server, const std::string &queue, const std::string &payload_bytes, const std::string &headers_bytes, zend_long delay_ms, zend_long max_attempts) {
    kislay_queue_config_t &config = kislay_queue_ensure_config_locked(server, queue);
    kislay_queue_stats_t &stats = kislay_queue_ensure_stats_locked(server, queue);

    kislay_queue_job_record_t job;
    job.id = kislay_queue_make_job_id(server);
    job.queue = queue;
    job.payload_bytes = payload_bytes;
    job.headers_bytes = headers_bytes;
    job.created_at_ms = kislay_now_ms();
    if (delay_ms < 0) {
        delay_ms = 0;
    }
    job.available_at_ms = job.created_at_ms + static_cast<std::uint64_t>(delay_ms);
    job.max_attempts = max_attempts > 0 ? max_attempts : config.max_attempts;
    job.status = delay_ms > 0 ? "delayed" : "ready";

    server->jobs[job.id] = job;
    server->queue_jobs[queue].push_back(job.id);
    stats.pushed_total++;
    return job.id;
}

static bool kislay_queue_fetch_locked(php_kislayphp_queue_server_t *server, const std::string &queue, zend_long lease_ms, const std::string &worker_id, kislay_queue_fetch_result_t *result) {
    kislay_queue_ensure_config_locked(server, queue);
    std::vector<std::string> &ids = server->queue_jobs[queue];
    std::uint64_t now_ms = kislay_now_ms();
    if (lease_ms <= 0) {
        lease_ms = kislay_queue_ensure_config_locked(server, queue).visibility_timeout_ms;
    }
    if (lease_ms <= 0) {
        lease_ms = 30000;
    }

    for (std::size_t i = 0; i < ids.size(); ++i) {
        std::unordered_map<std::string, kislay_queue_job_record_t>::iterator it = server->jobs.find(ids[i]);
        if (it == server->jobs.end()) {
            ids.erase(ids.begin() + i);
            i--;
            continue;
        }

        kislay_queue_job_record_t &job = it->second;
        std::string original_queue = job.queue;
        kislay_queue_reconcile_job_locked(server, &job, now_ms);
        if (job.queue != original_queue) {
            i--;
            continue;
        }
        if (job.status == "ready" && job.available_at_ms <= now_ms) {
            job.status = "leased";
            job.lease_until_ms = now_ms + static_cast<std::uint64_t>(lease_ms);
            job.worker_id = worker_id;
            job.attempts++;
            kislay_queue_ensure_stats_locked(server, queue).fetched_total++;

            result->found = true;
            result->id = job.id;
            result->queue = job.queue;
            result->payload_bytes = job.payload_bytes;
            result->headers_bytes = job.headers_bytes;
            result->attempts = job.attempts;
            result->max_attempts = job.max_attempts;
            result->available_at_ms = job.available_at_ms;
            return true;
        }
    }
    result->found = false;
    return true;
}

static void kislay_queue_collect_counts_locked(php_kislayphp_queue_server_t *server, const std::string &queue, std::uint64_t *ready, std::uint64_t *delayed, std::uint64_t *leased) {
    *ready = 0;
    *delayed = 0;
    *leased = 0;
    std::vector<std::string> &ids = server->queue_jobs[queue];
    std::uint64_t now_ms = kislay_now_ms();
    for (std::size_t i = 0; i < ids.size(); ++i) {
        std::unordered_map<std::string, kislay_queue_job_record_t>::iterator it = server->jobs.find(ids[i]);
        if (it == server->jobs.end()) {
            ids.erase(ids.begin() + i);
            i--;
            continue;
        }
        kislay_queue_job_record_t &job = it->second;
        std::string original_queue = job.queue;
        kislay_queue_reconcile_job_locked(server, &job, now_ms);
        if (job.queue != original_queue) {
            i--;
            continue;
        }
        if (job.status == "ready") {
            (*ready)++;
        } else if (job.status == "delayed") {
            (*delayed)++;
        } else if (job.status == "leased") {
            (*leased)++;
        }
    }
}

static void kislay_queue_stats_to_zval(php_kislayphp_queue_server_t *server, const std::string &queue, zval *return_value) {
    kislay_queue_stats_t &stats = kislay_queue_ensure_stats_locked(server, queue);
    std::uint64_t ready = 0;
    std::uint64_t delayed = 0;
    std::uint64_t leased = 0;
    kislay_queue_collect_counts_locked(server, queue, &ready, &delayed, &leased);

    array_init(return_value);
    add_assoc_string(return_value, "queue", const_cast<char *>(queue.c_str()));
    add_assoc_long(return_value, "ready", static_cast<zend_long>(ready));
    add_assoc_long(return_value, "delayed", static_cast<zend_long>(delayed));
    add_assoc_long(return_value, "leased", static_cast<zend_long>(leased));
    add_assoc_long(return_value, "pushed_total", static_cast<zend_long>(stats.pushed_total));
    add_assoc_long(return_value, "fetched_total", static_cast<zend_long>(stats.fetched_total));
    add_assoc_long(return_value, "acked_total", static_cast<zend_long>(stats.acked_total));
    add_assoc_long(return_value, "nacked_total", static_cast<zend_long>(stats.nacked_total));
    add_assoc_long(return_value, "retried_total", static_cast<zend_long>(stats.retried_total));
    add_assoc_long(return_value, "dead_lettered_total", static_cast<zend_long>(stats.dead_lettered_total));
    add_assoc_long(return_value, "purged_total", static_cast<zend_long>(stats.purged_total));
}

static zend_long kislay_queue_purge_locked(php_kislayphp_queue_server_t *server, const std::string &queue) {
    zend_long removed = 0;
    std::vector<std::string> &ids = server->queue_jobs[queue];
    for (std::size_t i = 0; i < ids.size(); ++i) {
        removed++;
        server->jobs.erase(ids[i]);
    }
    ids.clear();
    kislay_queue_ensure_stats_locked(server, queue).purged_total += static_cast<std::uint64_t>(removed);
    return removed;
}

static bool kislay_queue_build_json_body(zval *root, std::string *json, std::string *error) {
    if (kislay_json_encode_zval(root, json)) {
        return true;
    }
    if (error != nullptr) {
        *error = "Failed to encode JSON";
    }
    return false;
}

static bool kislay_queue_path_extract(const std::string &path, const std::string &prefix, const std::string &suffix, std::string *value) {
    if (path.size() < prefix.size() + suffix.size()) {
        return false;
    }
    if (path.compare(0, prefix.size(), prefix) != 0) {
        return false;
    }
    if (path.compare(path.size() - suffix.size(), suffix.size(), suffix) != 0) {
        return false;
    }
    *value = kislay_url_decode(path.substr(prefix.size(), path.size() - prefix.size() - suffix.size()));
    return true;
}

static bool kislay_queue_client_request_json(const std::string &method, const std::string &url, const std::string &body, zval *decoded, std::string *error) {
    int status = 0;
    std::string response_body;
    if (!kislay_http_request(method, url, body, &status, &response_body, error)) {
        return false;
    }
    if (status < 200 || status >= 300) {
        if (error != nullptr) {
            *error = "Queue server responded with HTTP " + std::to_string(status);
        }
        return false;
    }
    if (response_body.empty()) {
        array_init(decoded);
        return true;
    }
    if (!kislay_json_decode_assoc(response_body, decoded) || Z_TYPE_P(decoded) != IS_ARRAY) {
        if (!Z_ISUNDEF_P(decoded)) {
            zval_ptr_dtor(decoded);
        }
        if (error != nullptr) {
            *error = "Invalid JSON response";
        }
        return false;
    }
    return true;
}

static bool kislay_queue_client_push_remote(const std::string &base_url, const std::string &queue, zval *payload, zval *options, std::string *job_id, std::string *error) {
    std::string payload_bytes;
    if (!kislay_serialize_payload(payload, &payload_bytes)) {
        if (error != nullptr) {
            *error = "Unable to serialize payload";
        }
        return false;
    }

    std::string headers_bytes;
    zend_long delay_ms = 0;
    zend_long max_attempts = 0;
    if (options != nullptr && Z_TYPE_P(options) == IS_ARRAY) {
        HashTable *ht = Z_ARRVAL_P(options);
        delay_ms = kislay_hash_find_long(ht, "delay_ms", 0);
        max_attempts = kislay_hash_find_long(ht, "max_attempts", 0);
        zval *headers = kislay_hash_find(ht, "headers");
        if (headers != nullptr && Z_TYPE_P(headers) != IS_NULL) {
            if (!kislay_serialize_payload(headers, &headers_bytes)) {
                if (error != nullptr) {
                    *error = "Unable to serialize headers";
                }
                return false;
            }
        }
    }

    zval root;
    array_init(&root);
    add_assoc_string(&root, "payload_hex", const_cast<char *>(kislay_hex_encode(payload_bytes).c_str()));
    add_assoc_string(&root, "headers_hex", const_cast<char *>(kislay_hex_encode(headers_bytes).c_str()));
    add_assoc_long(&root, "delay_ms", delay_ms);
    if (max_attempts > 0) {
        add_assoc_long(&root, "max_attempts", max_attempts);
    }

    std::string json;
    bool ok = kislay_queue_build_json_body(&root, &json, error);
    zval_ptr_dtor(&root);
    if (!ok) {
        return false;
    }

    std::ostringstream url;
    url << base_url << "/v1/queues/" << queue << "/jobs";
    zval decoded;
    ZVAL_UNDEF(&decoded);
    if (!kislay_queue_client_request_json("POST", url.str(), json, &decoded, error)) {
        return false;
    }
    bool found = kislay_hash_find_string(Z_ARRVAL(decoded), "job_id", job_id);
    zval_ptr_dtor(&decoded);
    if (!found) {
        if (error != nullptr) {
            *error = "Queue server response missing job_id";
        }
        return false;
    }
    return true;
}

static bool kislay_queue_client_fetch_remote(const std::string &base_url, const std::string &queue, const std::string &worker_id, zend_long lease_ms, kislay_queue_fetch_result_t *result, std::string *error) {
    zval root;
    array_init(&root);
    add_assoc_string(&root, "worker_id", const_cast<char *>(worker_id.c_str()));
    add_assoc_long(&root, "lease_ms", lease_ms);
    std::string json;
    bool ok = kislay_queue_build_json_body(&root, &json, error);
    zval_ptr_dtor(&root);
    if (!ok) {
        return false;
    }

    std::ostringstream url;
    url << base_url << "/v1/queues/" << queue << "/fetch";
    zval decoded;
    ZVAL_UNDEF(&decoded);
    if (!kislay_queue_client_request_json("POST", url.str(), json, &decoded, error)) {
        return false;
    }

    zval *job = kislay_hash_find(Z_ARRVAL(decoded), "job");
    if (job == nullptr || Z_TYPE_P(job) == IS_NULL) {
        result->found = false;
        zval_ptr_dtor(&decoded);
        return true;
    }
    if (Z_TYPE_P(job) != IS_ARRAY) {
        zval_ptr_dtor(&decoded);
        if (error != nullptr) {
            *error = "Invalid job payload";
        }
        return false;
    }

    result->found = true;
    if (!kislay_hash_find_string(Z_ARRVAL_P(job), "id", &result->id) ||
        !kislay_hash_find_string(Z_ARRVAL_P(job), "queue", &result->queue)) {
        zval_ptr_dtor(&decoded);
        if (error != nullptr) {
            *error = "Job payload missing id or queue";
        }
        return false;
    }
    std::string payload_hex;
    std::string headers_hex;
    kislay_hash_find_string(Z_ARRVAL_P(job), "payload_hex", &payload_hex);
    kislay_hash_find_string(Z_ARRVAL_P(job), "headers_hex", &headers_hex);
    if (!kislay_hex_decode(payload_hex, &result->payload_bytes) || !kislay_hex_decode(headers_hex, &result->headers_bytes)) {
        zval_ptr_dtor(&decoded);
        if (error != nullptr) {
            *error = "Unable to decode job payload";
        }
        return false;
    }
    result->attempts = kislay_hash_find_long(Z_ARRVAL_P(job), "attempts", 0);
    result->max_attempts = kislay_hash_find_long(Z_ARRVAL_P(job), "max_attempts", 0);
    result->available_at_ms = static_cast<std::uint64_t>(kislay_hash_find_long(Z_ARRVAL_P(job), "available_at_ms", 0));
    zval_ptr_dtor(&decoded);
    return true;
}

static bool kislay_queue_client_ack_remote(const std::string &base_url, const std::string &job_id, std::string *error) {
    std::ostringstream url;
    url << base_url << "/v1/jobs/" << job_id << "/ack";
    zval decoded;
    ZVAL_UNDEF(&decoded);
    bool ok = kislay_queue_client_request_json("POST", url.str(), std::string("{}"), &decoded, error);
    if (ok) {
        zval_ptr_dtor(&decoded);
    }
    return ok;
}

static bool kislay_queue_client_nack_remote(const std::string &base_url, const std::string &job_id, bool requeue, zend_long delay_ms, std::string *error) {
    zval root;
    array_init(&root);
    add_assoc_bool(&root, "requeue", requeue ? 1 : 0);
    if (delay_ms >= 0) {
        add_assoc_long(&root, "delay_ms", delay_ms);
    }
    std::string json;
    bool ok = kislay_queue_build_json_body(&root, &json, error);
    zval_ptr_dtor(&root);
    if (!ok) {
        return false;
    }

    std::ostringstream url;
    url << base_url << "/v1/jobs/" << job_id << "/nack";
    zval decoded;
    ZVAL_UNDEF(&decoded);
    ok = kislay_queue_client_request_json("POST", url.str(), json, &decoded, error);
    if (ok) {
        zval_ptr_dtor(&decoded);
    }
    return ok;
}

static bool kislay_queue_fill_job_object(const std::string &base_url, const kislay_queue_fetch_result_t &fetched, zval *return_value, std::string *error) {
    object_init_ex(return_value, kislayphp_queue_job_ce);
    php_kislayphp_queue_job_t *obj = php_kislayphp_queue_job_from_obj(Z_OBJ_P(return_value));
    obj->base_url = base_url;
    obj->id = fetched.id;
    obj->queue = fetched.queue;
    obj->attempts = fetched.attempts;
    obj->max_attempts = fetched.max_attempts;
    obj->available_at_ms = fetched.available_at_ms;
    obj->finished = false;

    if (!kislay_unserialize_payload(fetched.payload_bytes, &obj->payload)) {
        if (error != nullptr) {
            *error = "Unable to unserialize job payload";
        }
        zval_ptr_dtor(return_value);
        ZVAL_NULL(return_value);
        return false;
    }

    if (fetched.headers_bytes.empty()) {
        array_init(&obj->headers);
    } else if (!kislay_unserialize_payload(fetched.headers_bytes, &obj->headers)) {
        if (error != nullptr) {
            *error = "Unable to unserialize job headers";
        }
        zval_ptr_dtor(return_value);
        ZVAL_NULL(return_value);
        return false;
    }

    return true;
}

static zend_object *kislayphp_queue_create_object(zend_class_entry *ce) {
    php_kislayphp_queue_t *obj = static_cast<php_kislayphp_queue_t *>(ecalloc(1, sizeof(php_kislayphp_queue_t) + zend_object_properties_size(ce)));
    zend_object_std_init(&obj->std, ce);
    object_properties_init(&obj->std, ce);
    new (&obj->queues) std::unordered_map<std::string, std::vector<zval>>();
    pthread_mutex_init(&obj->lock, nullptr);
    ZVAL_UNDEF(&obj->client);
    obj->has_client = false;
    obj->std.handlers = &kislayphp_queue_handlers;
    return &obj->std;
}

static void kislayphp_queue_free_obj(zend_object *object) {
    php_kislayphp_queue_t *obj = php_kislayphp_queue_from_obj(object);
    if (obj->has_client) {
        zval_ptr_dtor(&obj->client);
    }
    for (std::unordered_map<std::string, std::vector<zval>>::iterator it = obj->queues.begin(); it != obj->queues.end(); ++it) {
        for (std::size_t i = 0; i < it->second.size(); ++i) {
            zval_ptr_dtor(&it->second[i]);
        }
    }
    obj->queues.~unordered_map();
    pthread_mutex_destroy(&obj->lock);
    zend_object_std_dtor(&obj->std);
}

static zend_object *kislayphp_queue_server_create_object(zend_class_entry *ce) {
    php_kislayphp_queue_server_t *obj = static_cast<php_kislayphp_queue_server_t *>(ecalloc(1, sizeof(php_kislayphp_queue_server_t) + zend_object_properties_size(ce)));
    zend_object_std_init(&obj->std, ce);
    object_properties_init(&obj->std, ce);
    new (&obj->configs) std::unordered_map<std::string, kislay_queue_config_t>();
    new (&obj->queue_jobs) std::unordered_map<std::string, std::vector<std::string>>();
    new (&obj->jobs) std::unordered_map<std::string, kislay_queue_job_record_t>();
    new (&obj->stats) std::unordered_map<std::string, kislay_queue_stats_t>();
    new (&obj->host) std::string("127.0.0.1");
    obj->host = "127.0.0.1";
    obj->port = 9020;
    obj->listen_fd = -1;
    obj->running = false;
    obj->next_job_id = 0;
    pthread_mutex_init(&obj->lock, nullptr);
    obj->std.handlers = &kislayphp_queue_server_handlers;
    return &obj->std;
}

static void kislayphp_queue_server_free_obj(zend_object *object) {
    php_kislayphp_queue_server_t *obj = php_kislayphp_queue_server_from_obj(object);
    if (obj->listen_fd >= 0) {
        close(obj->listen_fd);
        obj->listen_fd = -1;
    }
    obj->configs.~unordered_map();
    obj->queue_jobs.~unordered_map();
    obj->jobs.~unordered_map();
    obj->stats.~unordered_map();
    pthread_mutex_destroy(&obj->lock);
    zend_object_std_dtor(&obj->std);
}

static zend_object *kislayphp_queue_client_create_object(zend_class_entry *ce) {
    php_kislayphp_queue_client_t *obj = static_cast<php_kislayphp_queue_client_t *>(ecalloc(1, sizeof(php_kislayphp_queue_client_t) + zend_object_properties_size(ce)));
    zend_object_std_init(&obj->std, ce);
    object_properties_init(&obj->std, ce);
    new (&obj->base_url) std::string("http://127.0.0.1:9020");
    obj->base_url = "http://127.0.0.1:9020";
    obj->std.handlers = &kislayphp_queue_client_handlers;
    return &obj->std;
}

static void kislayphp_queue_client_free_obj(zend_object *object) {
    php_kislayphp_queue_client_t *obj = php_kislayphp_queue_client_from_obj(object);
    obj->base_url.~basic_string();
    zend_object_std_dtor(&obj->std);
}

static zend_object *kislayphp_queue_worker_create_object(zend_class_entry *ce) {
    php_kislayphp_queue_worker_t *obj = static_cast<php_kislayphp_queue_worker_t *>(ecalloc(1, sizeof(php_kislayphp_queue_worker_t) + zend_object_properties_size(ce)));
    zend_object_std_init(&obj->std, ce);
    object_properties_init(&obj->std, ce);
    new (&obj->base_url) std::string("http://127.0.0.1:9020");
    new (&obj->stop_requested) std::atomic<bool>(false);
    obj->std.handlers = &kislayphp_queue_worker_handlers;
    return &obj->std;
}

static void kislayphp_queue_worker_free_obj(zend_object *object) {
    php_kislayphp_queue_worker_t *obj = php_kislayphp_queue_worker_from_obj(object);
    obj->base_url.~basic_string();
    obj->stop_requested.~atomic();
    zend_object_std_dtor(&obj->std);
}

static zend_object *kislayphp_queue_job_create_object(zend_class_entry *ce) {
    php_kislayphp_queue_job_t *obj = static_cast<php_kislayphp_queue_job_t *>(ecalloc(1, sizeof(php_kislayphp_queue_job_t) + zend_object_properties_size(ce)));
    zend_object_std_init(&obj->std, ce);
    object_properties_init(&obj->std, ce);
    new (&obj->base_url) std::string();
    new (&obj->id) std::string();
    new (&obj->queue) std::string();
    ZVAL_UNDEF(&obj->payload);
    ZVAL_UNDEF(&obj->headers);
    obj->attempts = 0;
    obj->max_attempts = 0;
    obj->available_at_ms = 0;
    obj->finished = false;
    obj->std.handlers = &kislayphp_queue_job_handlers;
    return &obj->std;
}

static void kislayphp_queue_job_free_obj(zend_object *object) {
    php_kislayphp_queue_job_t *obj = php_kislayphp_queue_job_from_obj(object);
    if (!Z_ISUNDEF(obj->payload)) {
        zval_ptr_dtor(&obj->payload);
    }
    if (!Z_ISUNDEF(obj->headers)) {
        zval_ptr_dtor(&obj->headers);
    }
    obj->base_url.~basic_string();
    obj->id.~basic_string();
    obj->queue.~basic_string();
    zend_object_std_dtor(&obj->std);
}

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_void, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_enqueue, 0, 0, 2)
    ZEND_ARG_TYPE_INFO(0, queue, IS_STRING, 0)
    ZEND_ARG_INFO(0, payload)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_dequeue, 0, 0, 1)
    ZEND_ARG_TYPE_INFO(0, queue, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_size, 0, 0, 1)
    ZEND_ARG_TYPE_INFO(0, queue, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_set_client, 0, 0, 1)
    ZEND_ARG_INFO(0, client)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_clear, 0, 0, 1)
    ZEND_ARG_TYPE_INFO(0, queue, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_server_construct, 0, 0, 0)
    ZEND_ARG_ARRAY_INFO(0, options, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_server_listen, 0, 0, 2)
    ZEND_ARG_TYPE_INFO(0, host, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, port, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_server_declare, 0, 0, 1)
    ZEND_ARG_TYPE_INFO(0, queue, IS_STRING, 0)
    ZEND_ARG_ARRAY_INFO(0, options, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_server_stats, 0, 0, 0)
    ZEND_ARG_TYPE_INFO(0, queue, IS_STRING, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_client_construct, 0, 0, 1)
    ZEND_ARG_TYPE_INFO(0, baseUrl, IS_STRING, 0)
    ZEND_ARG_ARRAY_INFO(0, options, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_client_push, 0, 0, 2)
    ZEND_ARG_TYPE_INFO(0, queue, IS_STRING, 0)
    ZEND_ARG_INFO(0, payload)
    ZEND_ARG_ARRAY_INFO(0, options, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_client_push_batch, 0, 0, 2)
    ZEND_ARG_TYPE_INFO(0, queue, IS_STRING, 0)
    ZEND_ARG_ARRAY_INFO(0, jobs, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_worker_consume, 0, 0, 2)
    ZEND_ARG_TYPE_INFO(0, queue, IS_STRING, 0)
    ZEND_ARG_CALLABLE_INFO(0, handler, 0)
    ZEND_ARG_ARRAY_INFO(0, options, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_job_nack, 0, 0, 0)
    ZEND_ARG_TYPE_INFO(0, requeue, _IS_BOOL, 1)
    ZEND_ARG_TYPE_INFO(0, delayMs, IS_LONG, 1)
ZEND_END_ARG_INFO()

PHP_METHOD(KislayPHPQueue, __construct) {
    ZEND_PARSE_PARAMETERS_NONE();
}

PHP_METHOD(KislayPHPQueue, setClient) {
    zval *client = nullptr;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_ZVAL(client)
    ZEND_PARSE_PARAMETERS_END();

    if (client == nullptr || Z_TYPE_P(client) != IS_OBJECT) {
        zend_throw_exception(zend_ce_exception, "Client must be an object", 0);
        RETURN_FALSE;
    }

    if (!instanceof_function(Z_OBJCE_P(client), kislayphp_queue_client_interface_ce)) {
        zend_throw_exception(zend_ce_exception, "Client must implement Kislay\\Queue\\ClientInterface", 0);
        RETURN_FALSE;
    }

    php_kislayphp_queue_t *obj = php_kislayphp_queue_from_obj(Z_OBJ_P(getThis()));
    if (obj->has_client) {
        zval_ptr_dtor(&obj->client);
        obj->has_client = false;
    }
    ZVAL_COPY(&obj->client, client);
    obj->has_client = true;
    RETURN_TRUE;
}

PHP_METHOD(KislayPHPQueue, enqueue) {
    char *queue = nullptr;
    size_t queue_len = 0;
    zval *payload = nullptr;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(queue, queue_len)
        Z_PARAM_ZVAL(payload)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_t *obj = php_kislayphp_queue_from_obj(Z_OBJ_P(getThis()));
    if (obj->has_client) {
        zval queue_zv;
        ZVAL_STRINGL(&queue_zv, queue, queue_len);
        zval retval;
        ZVAL_UNDEF(&retval);
        zend_call_method_with_2_params(Z_OBJ(obj->client), Z_OBJCE(obj->client), nullptr, "enqueue", &retval, &queue_zv, payload);
        zval_ptr_dtor(&queue_zv);
        if (Z_ISUNDEF(retval)) {
            RETURN_TRUE;
        }
        RETVAL_ZVAL(&retval, 1, 1);
        return;
    }

    zval copy;
    ZVAL_COPY(&copy, payload);
    pthread_mutex_lock(&obj->lock);
    obj->queues[std::string(queue, queue_len)].push_back(copy);
    pthread_mutex_unlock(&obj->lock);
    RETURN_TRUE;
}

PHP_METHOD(KislayPHPQueue, dequeue) {
    char *queue = nullptr;
    size_t queue_len = 0;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(queue, queue_len)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_t *obj = php_kislayphp_queue_from_obj(Z_OBJ_P(getThis()));
    if (obj->has_client) {
        zval queue_zv;
        ZVAL_STRINGL(&queue_zv, queue, queue_len);
        zval retval;
        ZVAL_UNDEF(&retval);
        zend_call_method_with_1_params(Z_OBJ(obj->client), Z_OBJCE(obj->client), nullptr, "dequeue", &retval, &queue_zv);
        zval_ptr_dtor(&queue_zv);
        if (Z_ISUNDEF(retval)) {
            RETURN_NULL();
        }
        RETVAL_ZVAL(&retval, 1, 1);
        return;
    }

    pthread_mutex_lock(&obj->lock);
    std::unordered_map<std::string, std::vector<zval>>::iterator it = obj->queues.find(std::string(queue, queue_len));
    if (it == obj->queues.end() || it->second.empty()) {
        pthread_mutex_unlock(&obj->lock);
        RETURN_NULL();
    }
    zval &item = it->second.front();
    ZVAL_COPY(return_value, &item);
    zval_ptr_dtor(&item);
    it->second.erase(it->second.begin());
    pthread_mutex_unlock(&obj->lock);
}

PHP_METHOD(KislayPHPQueue, size) {
    char *queue = nullptr;
    size_t queue_len = 0;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(queue, queue_len)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_t *obj = php_kislayphp_queue_from_obj(Z_OBJ_P(getThis()));
    if (obj->has_client) {
        zval queue_zv;
        ZVAL_STRINGL(&queue_zv, queue, queue_len);
        zval retval;
        ZVAL_UNDEF(&retval);
        zend_call_method_with_1_params(Z_OBJ(obj->client), Z_OBJCE(obj->client), nullptr, "size", &retval, &queue_zv);
        zval_ptr_dtor(&queue_zv);
        if (Z_ISUNDEF(retval)) {
            RETURN_LONG(0);
        }
        RETVAL_ZVAL(&retval, 1, 1);
        return;
    }

    pthread_mutex_lock(&obj->lock);
    std::unordered_map<std::string, std::vector<zval>>::iterator it = obj->queues.find(std::string(queue, queue_len));
    if (it == obj->queues.end()) {
        pthread_mutex_unlock(&obj->lock);
        RETURN_LONG(0);
    }
    zend_long count = static_cast<zend_long>(it->second.size());
    pthread_mutex_unlock(&obj->lock);
    RETURN_LONG(count);
}

PHP_METHOD(KislayPHPQueue, peek) {
    char *queue = nullptr;
    size_t queue_len = 0;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(queue, queue_len)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_t *obj = php_kislayphp_queue_from_obj(Z_OBJ_P(getThis()));
    pthread_mutex_lock(&obj->lock);
    std::unordered_map<std::string, std::vector<zval>>::iterator it = obj->queues.find(std::string(queue, queue_len));
    if (it == obj->queues.end() || it->second.empty()) {
        pthread_mutex_unlock(&obj->lock);
        RETURN_NULL();
    }
    ZVAL_COPY(return_value, &it->second.front());
    pthread_mutex_unlock(&obj->lock);
}

PHP_METHOD(KislayPHPQueue, clear) {
    char *queue = nullptr;
    size_t queue_len = 0;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(queue, queue_len)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_t *obj = php_kislayphp_queue_from_obj(Z_OBJ_P(getThis()));
    zend_long removed = 0;
    pthread_mutex_lock(&obj->lock);
    std::unordered_map<std::string, std::vector<zval>>::iterator it = obj->queues.find(std::string(queue, queue_len));
    if (it != obj->queues.end()) {
        removed = static_cast<zend_long>(it->second.size());
        for (std::size_t i = 0; i < it->second.size(); ++i) {
            zval_ptr_dtor(&it->second[i]);
        }
        obj->queues.erase(it);
    }
    pthread_mutex_unlock(&obj->lock);
    RETURN_LONG(removed);
}

PHP_METHOD(KislayPHPQueueServer, __construct) {
    zval *options = nullptr;
    ZEND_PARSE_PARAMETERS_START(0, 1)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY_EX(options, 1, 0)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_server_t *obj = php_kislayphp_queue_server_from_obj(Z_OBJ_P(getThis()));
    if (options != nullptr) {
        HashTable *ht = Z_ARRVAL_P(options);
        std::string host;
        if (kislay_hash_find_string(ht, "host", &host)) {
            obj->host = host;
        }
        obj->port = kislay_hash_find_long(ht, "port", obj->port);
    }
}

PHP_METHOD(KislayPHPQueueServer, listen) {
    char *host = nullptr;
    size_t host_len = 0;
    zend_long port = 0;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(host, host_len)
        Z_PARAM_LONG(port)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_server_t *obj = php_kislayphp_queue_server_from_obj(Z_OBJ_P(getThis()));
    obj->host.assign(host, host_len);
    obj->port = port;
    RETURN_TRUE;
}

PHP_METHOD(KislayPHPQueueServer, declare) {
    char *queue = nullptr;
    size_t queue_len = 0;
    zval *options = nullptr;
    ZEND_PARSE_PARAMETERS_START(1, 2)
        Z_PARAM_STRING(queue, queue_len)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY_EX(options, 1, 0)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_server_t *obj = php_kislayphp_queue_server_from_obj(Z_OBJ_P(getThis()));
    kislay_scoped_lock_t guard(&obj->lock);
    kislay_queue_config_t &config = kislay_queue_ensure_config_locked(obj, std::string(queue, queue_len));
    if (options != nullptr) {
        HashTable *ht = Z_ARRVAL_P(options);
        config.visibility_timeout_ms = kislay_hash_find_long(ht, "visibility_timeout_ms", config.visibility_timeout_ms);
        config.max_attempts = kislay_hash_find_long(ht, "max_attempts", config.max_attempts);
        config.retry_backoff_ms = kislay_hash_find_long(ht, "retry_backoff_ms", config.retry_backoff_ms);
        std::string dead_letter_queue;
        if (kislay_hash_find_string(ht, "dead_letter_queue", &dead_letter_queue)) {
            config.dead_letter_queue = dead_letter_queue;
        }
    }
    RETURN_TRUE;
}

PHP_METHOD(KislayPHPQueueServer, stats) {
    char *queue = nullptr;
    size_t queue_len = 0;
    ZEND_PARSE_PARAMETERS_START(0, 1)
        Z_PARAM_OPTIONAL
        Z_PARAM_STRING(queue, queue_len)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_server_t *obj = php_kislayphp_queue_server_from_obj(Z_OBJ_P(getThis()));
    kislay_scoped_lock_t guard(&obj->lock);
    if (queue != nullptr) {
        kislay_queue_stats_to_zval(obj, std::string(queue, queue_len), return_value);
        return;
    }

    array_init(return_value);
    for (std::unordered_map<std::string, std::vector<std::string>>::iterator it = obj->queue_jobs.begin(); it != obj->queue_jobs.end(); ++it) {
        zval entry;
        ZVAL_UNDEF(&entry);
        kislay_queue_stats_to_zval(obj, it->first, &entry);
        add_assoc_zval(return_value, it->first.c_str(), &entry);
    }
}

PHP_METHOD(KislayPHPQueueServer, run) {
    ZEND_PARSE_PARAMETERS_NONE();

    php_kislayphp_queue_server_t *obj = php_kislayphp_queue_server_from_obj(Z_OBJ_P(getThis()));

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        zend_throw_exception(zend_ce_exception, "Unable to create queue server socket", 0);
        return;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in address;
    std::memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<uint16_t>(obj->port));
    if (inet_pton(AF_INET, obj->host.c_str(), &address.sin_addr) <= 0) {
        close(server_fd);
        zend_throw_exception(zend_ce_exception, "Invalid queue server host", 0);
        return;
    }

    if (bind(server_fd, reinterpret_cast<struct sockaddr *>(&address), sizeof(address)) != 0) {
        close(server_fd);
        zend_throw_exception(zend_ce_exception, "Unable to bind queue server", 0);
        return;
    }

    if (listen(server_fd, 128) != 0) {
        close(server_fd);
        zend_throw_exception(zend_ce_exception, "Unable to listen on queue server socket", 0);
        return;
    }

    obj->listen_fd = server_fd;
    obj->running = true;

    while (obj->running) {
        int client_fd = accept(server_fd, nullptr, nullptr);
        if (client_fd < 0) {
            if (!obj->running) break;
            continue;
        }

        // Dispatch each client connection to its own thread so a slow client never
        // blocks the accept loop (or any other active client).
        std::thread([obj, client_fd]() {
        kislay_http_request_t request;
        if (!kislay_http_read_request(client_fd, &request)) {
            kislay_http_send_response(client_fd, 400, "application/json", "{\"error\":\"invalid request\"}");
            close(client_fd);
            return;
        }

        if (request.path == "/health") {
            kislay_http_send_response(client_fd, 200, "text/plain", "OK");
            close(client_fd);
            return;
        }

        // Every branch below this point may touch the Zend engine (JSON
        // decode/encode, zval construction) - see kislay_queue_php_call_lock's
        // definition for why this must be held across all of it, including
        // the response send (a fast local write, unlike socket's long-poll
        // wait, so holding the lock through it is an accepted, bounded cost).
        std::lock_guard<std::recursive_mutex> php_guard(kislay_queue_php_call_lock);

        std::string queue_name;
        std::string job_id;

        if (request.method == "POST" && kislay_queue_path_extract(request.path, "/v1/queues/", "/jobs", &queue_name)) {
            zval decoded;
            ZVAL_UNDEF(&decoded);
            if (!kislay_json_decode_assoc(request.body, &decoded) || Z_TYPE(decoded) != IS_ARRAY) {
                if (!Z_ISUNDEF(decoded)) {
                    zval_ptr_dtor(&decoded);
                }
                kislay_http_send_response(client_fd, 400, "application/json", "{\"error\":\"invalid json\"}");
                close(client_fd);
                return;
            }
            std::string payload_hex;
            std::string headers_hex;
            kislay_hash_find_string(Z_ARRVAL(decoded), "payload_hex", &payload_hex);
            kislay_hash_find_string(Z_ARRVAL(decoded), "headers_hex", &headers_hex);
            std::string payload_bytes;
            std::string headers_bytes;
            zend_long delay_ms = kislay_hash_find_long(Z_ARRVAL(decoded), "delay_ms", 0);
            zend_long max_attempts = kislay_hash_find_long(Z_ARRVAL(decoded), "max_attempts", 0);
            if (!kislay_hex_decode(payload_hex, &payload_bytes) || !kislay_hex_decode(headers_hex, &headers_bytes)) {
                zval_ptr_dtor(&decoded);
                kislay_http_send_response(client_fd, 400, "application/json", "{\"error\":\"invalid payload encoding\"}");
                close(client_fd);
                return;
            }
            std::string created_id;
            {
                kislay_scoped_lock_t guard(&obj->lock);
                created_id = kislay_queue_push_locked(obj, queue_name, payload_bytes, headers_bytes, delay_ms, max_attempts);
            }
            zval_ptr_dtor(&decoded);
            std::string body = std::string("{\"job_id\":\"") + created_id + "\"}";
            kislay_http_send_response(client_fd, 201, "application/json", body);
            close(client_fd);
            return;
        }

        if (request.method == "POST" && kislay_queue_path_extract(request.path, "/v1/queues/", "/fetch", &queue_name)) {
            zval decoded;
            ZVAL_UNDEF(&decoded);
            if (!kislay_json_decode_assoc(request.body, &decoded) || Z_TYPE(decoded) != IS_ARRAY) {
                if (!Z_ISUNDEF(decoded)) {
                    zval_ptr_dtor(&decoded);
                }
                kislay_http_send_response(client_fd, 400, "application/json", "{\"error\":\"invalid json\"}");
                close(client_fd);
                return;
            }
            std::string worker_id = "worker";
            kislay_hash_find_string(Z_ARRVAL(decoded), "worker_id", &worker_id);
            zend_long lease_ms = kislay_hash_find_long(Z_ARRVAL(decoded), "lease_ms", 0);
            kislay_queue_fetch_result_t fetched;
            {
                kislay_scoped_lock_t guard(&obj->lock);
                kislay_queue_fetch_locked(obj, queue_name, lease_ms, worker_id, &fetched);
            }
            zval_ptr_dtor(&decoded);

            if (!fetched.found) {
                kislay_http_send_response(client_fd, 200, "application/json", "{\"job\":null}");
                close(client_fd);
                return;
            }

            zval root;
            zval job;
            array_init(&root);
            array_init(&job);
            add_assoc_string(&job, "id", const_cast<char *>(fetched.id.c_str()));
            add_assoc_string(&job, "queue", const_cast<char *>(fetched.queue.c_str()));
            add_assoc_string(&job, "payload_hex", const_cast<char *>(kislay_hex_encode(fetched.payload_bytes).c_str()));
            add_assoc_string(&job, "headers_hex", const_cast<char *>(kislay_hex_encode(fetched.headers_bytes).c_str()));
            add_assoc_long(&job, "attempts", fetched.attempts);
            add_assoc_long(&job, "max_attempts", fetched.max_attempts);
            add_assoc_long(&job, "available_at_ms", static_cast<zend_long>(fetched.available_at_ms));
            add_assoc_zval(&root, "job", &job);
            std::string json;
            if (!kislay_json_encode_zval(&root, &json)) {
                zval_ptr_dtor(&root);
                kislay_http_send_response(client_fd, 500, "application/json", "{\"error\":\"encode failed\"}");
                close(client_fd);
                return;
            }
            zval_ptr_dtor(&root);
            kislay_http_send_response(client_fd, 200, "application/json", json);
            close(client_fd);
            return;
        }

        if (request.method == "GET" && kislay_queue_path_extract(request.path, "/v1/queues/", "/stats", &queue_name)) {
            zval root;
            ZVAL_UNDEF(&root);
            {
                kislay_scoped_lock_t guard(&obj->lock);
                kislay_queue_stats_to_zval(obj, queue_name, &root);
            }
            std::string json;
            if (!kislay_json_encode_zval(&root, &json)) {
                zval_ptr_dtor(&root);
                kislay_http_send_response(client_fd, 500, "application/json", "{\"error\":\"encode failed\"}");
                close(client_fd);
                return;
            }
            zval_ptr_dtor(&root);
            kislay_http_send_response(client_fd, 200, "application/json", json);
            close(client_fd);
            return;
        }

        if (request.method == "POST" && kislay_queue_path_extract(request.path, "/v1/queues/", "/purge", &queue_name)) {
            zend_long removed = 0;
            {
                kislay_scoped_lock_t guard(&obj->lock);
                removed = kislay_queue_purge_locked(obj, queue_name);
            }
            std::ostringstream payload;
            payload << "{\"removed\":" << removed << "}";
            kislay_http_send_response(client_fd, 200, "application/json", payload.str());
            close(client_fd);
            return;
        }

        if (request.method == "POST" && kislay_queue_path_extract(request.path, "/v1/jobs/", "/ack", &job_id)) {
            bool ok = false;
            {
                kislay_scoped_lock_t guard(&obj->lock);
                ok = kislay_queue_job_ack_locked(obj, job_id);
            }
            kislay_http_send_response(client_fd, ok ? 200 : 404, "application/json", ok ? "{\"ok\":true}" : "{\"error\":\"job not found\"}");
            close(client_fd);
            return;
        }

        if (request.method == "POST" && kislay_queue_path_extract(request.path, "/v1/jobs/", "/nack", &job_id)) {
            zval decoded;
            ZVAL_UNDEF(&decoded);
            if (!request.body.empty() && (!kislay_json_decode_assoc(request.body, &decoded) || Z_TYPE(decoded) != IS_ARRAY)) {
                if (!Z_ISUNDEF(decoded)) {
                    zval_ptr_dtor(&decoded);
                }
                kislay_http_send_response(client_fd, 400, "application/json", "{\"error\":\"invalid json\"}");
                close(client_fd);
                return;
            }
            bool requeue = true;
            zend_long delay_ms = -1;
            if (!Z_ISUNDEF(decoded)) {
                requeue = kislay_hash_find_bool(Z_ARRVAL(decoded), "requeue", true);
                zval *delay = kislay_hash_find(Z_ARRVAL(decoded), "delay_ms");
                if (delay != nullptr && Z_TYPE_P(delay) != IS_NULL) {
                    delay_ms = zval_get_long(delay);
                }
            }
            bool ok = false;
            {
                kislay_scoped_lock_t guard(&obj->lock);
                ok = kislay_queue_job_nack_locked(obj, job_id, requeue, delay_ms);
            }
            if (!Z_ISUNDEF(decoded)) {
                zval_ptr_dtor(&decoded);
            }
            kislay_http_send_response(client_fd, ok ? 200 : 404, "application/json", ok ? "{\"ok\":true}" : "{\"error\":\"job not found\"}");
            close(client_fd);
            return;
        }

        // Batch push: POST /v1/queues/{queue}/jobs/batch
        // Body: {"jobs":[{"payload":...,"delay_ms":0}, ...]}
        std::string batch_queue;
        if (request.method == "POST" &&
            kislay_queue_path_extract(request.path, "/v1/queues/", "/jobs/batch", &batch_queue)) {
            zval decoded;
            ZVAL_UNDEF(&decoded);
            if (!kislay_json_decode_assoc(request.body, &decoded) || Z_TYPE(decoded) != IS_ARRAY) {
                kislay_http_send_response(client_fd, 400, "application/json", "{\"error\":\"invalid json\"}");
                close(client_fd);
                return;
            }
            zval *jobs_arr = zend_hash_str_find(Z_ARRVAL(decoded), "jobs", sizeof("jobs")-1);
            if (!jobs_arr || Z_TYPE_P(jobs_arr) != IS_ARRAY) {
                zval_ptr_dtor(&decoded);
                kislay_http_send_response(client_fd, 400, "application/json", "{\"error\":\"missing jobs array\"}");
                close(client_fd);
                return;
            }
            std::vector<std::string> ids;
            zval *job_item;
            ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(jobs_arr), job_item) {
                if (Z_TYPE_P(job_item) != IS_ARRAY) continue;
                zval *payload_zv = zend_hash_str_find(Z_ARRVAL_P(job_item), "payload", sizeof("payload")-1);
                zend_long delay_ms = 0;
                zval *delay_zv = zend_hash_str_find(Z_ARRVAL_P(job_item), "delay_ms", sizeof("delay_ms")-1);
                if (delay_zv && Z_TYPE_P(delay_zv) == IS_LONG) delay_ms = Z_LVAL_P(delay_zv);
                std::string payload_bytes;
                if (payload_zv) {
                    kislay_json_encode_zval(payload_zv, &payload_bytes);
                } else {
                    payload_bytes = "{}";
                }
                kislay_scoped_lock_t guard(&obj->lock);
                std::string job_id = "job-" + std::to_string(obj->next_job_id++);
                kislay_queue_job_record_t rec;
                rec.id = job_id;
                rec.queue = batch_queue;
                rec.payload_bytes = payload_bytes;
                rec.status = "ready";
                std::uint64_t now_ms = kislay_now_ms();
                rec.created_at_ms = now_ms;
                rec.available_at_ms = now_ms + static_cast<std::uint64_t>(delay_ms > 0 ? delay_ms : 0);
                auto &config = obj->configs[batch_queue];
                rec.max_attempts = config.max_attempts;
                obj->jobs[job_id] = std::move(rec);
                obj->queue_jobs[batch_queue].push_back(job_id);
                obj->stats[batch_queue].pushed_total++;
                ids.push_back(job_id);
            } ZEND_HASH_FOREACH_END();
            zval_ptr_dtor(&decoded);
            std::ostringstream resp;
            resp << "{\"count\":"  << ids.size() << ",\"ids\":[";
            for (std::size_t i = 0; i < ids.size(); ++i) {
                if (i > 0) resp << ",";
                resp << "\"" << ids[i] << "\"";
            }
            resp << "]}";
            kislay_http_send_response(client_fd, 201, "application/json", resp.str());
            close(client_fd);
            return;
        }

        kislay_http_send_response(client_fd, 404, "application/json", "{\"error\":\"not found\"}");
        close(client_fd);
        }).detach();
    }

    if (obj->listen_fd >= 0) {
        close(obj->listen_fd);
        obj->listen_fd = -1;
    }
    obj->running = false;
}

PHP_METHOD(KislayPHPQueueServer, stop) {
    ZEND_PARSE_PARAMETERS_NONE();
    php_kislayphp_queue_server_t *obj = php_kislayphp_queue_server_from_obj(Z_OBJ_P(getThis()));
    obj->running = false;
    if (obj->listen_fd >= 0) {
        shutdown(obj->listen_fd, SHUT_RDWR);
        close(obj->listen_fd);
        obj->listen_fd = -1;
    }
    RETURN_TRUE;
}

PHP_METHOD(KislayPHPQueueClient, __construct) {
    char *base_url = nullptr;
    size_t base_url_len = 0;
    zval *options = nullptr;
    ZEND_PARSE_PARAMETERS_START(1, 2)
        Z_PARAM_STRING(base_url, base_url_len)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY_EX(options, 1, 0)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_client_t *obj = php_kislayphp_queue_client_from_obj(Z_OBJ_P(getThis()));
    obj->base_url.assign(base_url, base_url_len);
    (void) options;
}

PHP_METHOD(KislayPHPQueueClient, push) {
    char *queue = nullptr;
    size_t queue_len = 0;
    zval *payload = nullptr;
    zval *options = nullptr;
    ZEND_PARSE_PARAMETERS_START(2, 3)
        Z_PARAM_STRING(queue, queue_len)
        Z_PARAM_ZVAL(payload)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY_EX(options, 1, 0)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_client_t *obj = php_kislayphp_queue_client_from_obj(Z_OBJ_P(getThis()));
    std::string job_id;
    std::string error;
    if (!kislay_queue_client_push_remote(obj->base_url, std::string(queue, queue_len), payload, options, &job_id, &error)) {
        zend_throw_exception(zend_ce_exception, error.c_str(), 0);
        return;
    }
    RETURN_STRINGL(job_id.c_str(), job_id.size());
}

PHP_METHOD(KislayPHPQueueClient, pushBatch) {
    char *queue = nullptr;
    size_t queue_len = 0;
    zval *jobs = nullptr;
    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(queue, queue_len)
        Z_PARAM_ARRAY(jobs)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_client_t *obj = php_kislayphp_queue_client_from_obj(Z_OBJ_P(getThis()));
    array_init(return_value);

    zval *entry = nullptr;
    ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(jobs), entry) {
        zval *payload = entry;
        zval *options = nullptr;
        if (Z_TYPE_P(entry) == IS_ARRAY) {
            zval *maybe_payload = kislay_hash_find(Z_ARRVAL_P(entry), "payload");
            if (maybe_payload != nullptr) {
                payload = maybe_payload;
                options = kislay_hash_find(Z_ARRVAL_P(entry), "options");
                if (options != nullptr && Z_TYPE_P(options) != IS_ARRAY) {
                    options = nullptr;
                }
            }
        }
        std::string job_id;
        std::string error;
        if (!kislay_queue_client_push_remote(obj->base_url, std::string(queue, queue_len), payload, options, &job_id, &error)) {
            zend_throw_exception(zend_ce_exception, error.c_str(), 0);
            return;
        }
        add_next_index_string(return_value, const_cast<char *>(job_id.c_str()));
    } ZEND_HASH_FOREACH_END();
}

PHP_METHOD(KislayPHPQueueClient, stats) {
    char *queue = nullptr;
    size_t queue_len = 0;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(queue, queue_len)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_client_t *obj = php_kislayphp_queue_client_from_obj(Z_OBJ_P(getThis()));
    std::ostringstream url;
    url << obj->base_url << "/v1/queues/" << std::string(queue, queue_len) << "/stats";
    std::string error;
    zval decoded;
    ZVAL_UNDEF(&decoded);
    if (!kislay_queue_client_request_json("GET", url.str(), std::string(), &decoded, &error)) {
        zend_throw_exception(zend_ce_exception, error.c_str(), 0);
        return;
    }
    RETVAL_ZVAL(&decoded, 1, 1);
}

PHP_METHOD(KislayPHPQueueClient, purge) {
    char *queue = nullptr;
    size_t queue_len = 0;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(queue, queue_len)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_client_t *obj = php_kislayphp_queue_client_from_obj(Z_OBJ_P(getThis()));
    std::ostringstream url;
    url << obj->base_url << "/v1/queues/" << std::string(queue, queue_len) << "/purge";
    std::string error;
    zval decoded;
    ZVAL_UNDEF(&decoded);
    if (!kislay_queue_client_request_json("POST", url.str(), std::string("{}"), &decoded, &error)) {
        zend_throw_exception(zend_ce_exception, error.c_str(), 0);
        return;
    }
    zend_long removed = kislay_hash_find_long(Z_ARRVAL(decoded), "removed", 0);
    zval_ptr_dtor(&decoded);
    RETURN_LONG(removed);
}

PHP_METHOD(KislayPHPQueueWorker, __construct) {
    char *base_url = nullptr;
    size_t base_url_len = 0;
    zval *options = nullptr;
    ZEND_PARSE_PARAMETERS_START(1, 2)
        Z_PARAM_STRING(base_url, base_url_len)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY_EX(options, 1, 0)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_worker_t *obj = php_kislayphp_queue_worker_from_obj(Z_OBJ_P(getThis()));
    obj->base_url.assign(base_url, base_url_len);
    obj->stop_requested.store(false, std::memory_order_relaxed);
    (void) options;
}

PHP_METHOD(KislayPHPQueueWorker, consume) {
    char *queue = nullptr;
    size_t queue_len = 0;
    zval *handler = nullptr;
    zval *options = nullptr;
    ZEND_PARSE_PARAMETERS_START(2, 3)
        Z_PARAM_STRING(queue, queue_len)
        Z_PARAM_ZVAL(handler)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY_EX(options, 1, 0)
    ZEND_PARSE_PARAMETERS_END();

    if (!zend_is_callable(handler, 0, nullptr)) {
        zend_throw_exception(zend_ce_exception, "Handler must be callable", 0);
        return;
    }

    php_kislayphp_queue_worker_t *obj = php_kislayphp_queue_worker_from_obj(Z_OBJ_P(getThis()));
    obj->stop_requested.store(false, std::memory_order_relaxed);

    zend_long poll_interval_ms = 250;
    zend_long lease_ms = 30000;
    bool stop_when_empty = false;
    std::string worker_id = std::string("worker-") + std::to_string(static_cast<unsigned long long>(kislay_now_ms()));
    if (options != nullptr) {
        HashTable *ht = Z_ARRVAL_P(options);
        poll_interval_ms = kislay_hash_find_long(ht, "poll_interval_ms", poll_interval_ms);
        lease_ms = kislay_hash_find_long(ht, "lease_ms", lease_ms);
        stop_when_empty = kislay_hash_find_bool(ht, "stop_when_empty", stop_when_empty);
        std::string configured_worker_id;
        if (kislay_hash_find_string(ht, "worker_id", &configured_worker_id)) {
            worker_id = configured_worker_id;
        }
    }

    while (!obj->stop_requested) {
        kislay_queue_fetch_result_t fetched;
        std::string error;
        if (!kislay_queue_client_fetch_remote(obj->base_url, std::string(queue, queue_len), worker_id, lease_ms, &fetched, &error)) {
            zend_throw_exception(zend_ce_exception, error.c_str(), 0);
            return;
        }

        if (!fetched.found) {
            if (stop_when_empty) {
                RETURN_TRUE;
            }
            kislay_sleep_ms(poll_interval_ms);
            continue;
        }

        zval job_zv;
        ZVAL_UNDEF(&job_zv);
        if (!kislay_queue_fill_job_object(obj->base_url, fetched, &job_zv, &error)) {
            zend_throw_exception(zend_ce_exception, error.c_str(), 0);
            return;
        }

        php_kislayphp_queue_job_t *job_obj = php_kislayphp_queue_job_from_obj(Z_OBJ(job_zv));
        zval callable;
        zval retval;
        zval params[1];
        ZVAL_COPY(&callable, handler);
        ZVAL_COPY(&params[0], &job_zv);
        ZVAL_UNDEF(&retval);

        int call_result = call_user_function(EG(function_table), nullptr, &callable, &retval, 1, params);
        zval_ptr_dtor(&callable);
        zval_ptr_dtor(&params[0]);

        if (call_result != SUCCESS || EG(exception)) {
            if (!job_obj->finished) {
                std::string nack_error;
                kislay_queue_client_nack_remote(job_obj->base_url, job_obj->id, true, -1, &nack_error);
                job_obj->finished = true;
            }
            if (!Z_ISUNDEF(retval)) {
                zval_ptr_dtor(&retval);
            }
            zval_ptr_dtor(&job_zv);
            return;
        }

        bool should_ack = !Z_ISUNDEF(retval) && zend_is_true(&retval);
        if (!Z_ISUNDEF(retval)) {
            zval_ptr_dtor(&retval);
        }

        if (!job_obj->finished) {
            std::string remote_error;
            bool ok = should_ack
                ? kislay_queue_client_ack_remote(job_obj->base_url, job_obj->id, &remote_error)
                : kislay_queue_client_nack_remote(job_obj->base_url, job_obj->id, true, -1, &remote_error);
            if (!ok) {
                zval_ptr_dtor(&job_zv);
                zend_throw_exception(zend_ce_exception, remote_error.c_str(), 0);
                return;
            }
            job_obj->finished = true;
        }

        zval_ptr_dtor(&job_zv);
    }

    RETURN_TRUE;
}

PHP_METHOD(KislayPHPQueueWorker, stop) {
    ZEND_PARSE_PARAMETERS_NONE();
    php_kislayphp_queue_worker_t *obj = php_kislayphp_queue_worker_from_obj(Z_OBJ_P(getThis()));
    obj->stop_requested = true;
    RETURN_TRUE;
}

PHP_METHOD(KislayPHPQueueJob, id) {
    ZEND_PARSE_PARAMETERS_NONE();
    php_kislayphp_queue_job_t *obj = php_kislayphp_queue_job_from_obj(Z_OBJ_P(getThis()));
    RETURN_STRINGL(obj->id.c_str(), obj->id.size());
}

PHP_METHOD(KislayPHPQueueJob, queue) {
    ZEND_PARSE_PARAMETERS_NONE();
    php_kislayphp_queue_job_t *obj = php_kislayphp_queue_job_from_obj(Z_OBJ_P(getThis()));
    RETURN_STRINGL(obj->queue.c_str(), obj->queue.size());
}

PHP_METHOD(KislayPHPQueueJob, payload) {
    ZEND_PARSE_PARAMETERS_NONE();
    php_kislayphp_queue_job_t *obj = php_kislayphp_queue_job_from_obj(Z_OBJ_P(getThis()));
    RETVAL_ZVAL(&obj->payload, 1, 0);
}

PHP_METHOD(KislayPHPQueueJob, headers) {
    ZEND_PARSE_PARAMETERS_NONE();
    php_kislayphp_queue_job_t *obj = php_kislayphp_queue_job_from_obj(Z_OBJ_P(getThis()));
    RETVAL_ZVAL(&obj->headers, 1, 0);
}

PHP_METHOD(KislayPHPQueueJob, attempts) {
    ZEND_PARSE_PARAMETERS_NONE();
    php_kislayphp_queue_job_t *obj = php_kislayphp_queue_job_from_obj(Z_OBJ_P(getThis()));
    RETURN_LONG(obj->attempts);
}

PHP_METHOD(KislayPHPQueueJob, maxAttempts) {
    ZEND_PARSE_PARAMETERS_NONE();
    php_kislayphp_queue_job_t *obj = php_kislayphp_queue_job_from_obj(Z_OBJ_P(getThis()));
    RETURN_LONG(obj->max_attempts);
}

PHP_METHOD(KislayPHPQueueJob, availableAt) {
    ZEND_PARSE_PARAMETERS_NONE();
    php_kislayphp_queue_job_t *obj = php_kislayphp_queue_job_from_obj(Z_OBJ_P(getThis()));
    RETURN_LONG(static_cast<zend_long>(obj->available_at_ms));
}

PHP_METHOD(KislayPHPQueueJob, ack) {
    ZEND_PARSE_PARAMETERS_NONE();
    php_kislayphp_queue_job_t *obj = php_kislayphp_queue_job_from_obj(Z_OBJ_P(getThis()));
    if (obj->finished) {
        RETURN_FALSE;
    }
    std::string error;
    if (!kislay_queue_client_ack_remote(obj->base_url, obj->id, &error)) {
        zend_throw_exception(zend_ce_exception, error.c_str(), 0);
        return;
    }
    obj->finished = true;
    RETURN_TRUE;
}

PHP_METHOD(KislayPHPQueueJob, nack) {
    zval *requeue = nullptr;
    zval *delay_ms = nullptr;
    ZEND_PARSE_PARAMETERS_START(0, 2)
        Z_PARAM_OPTIONAL
        Z_PARAM_ZVAL(requeue)
        Z_PARAM_ZVAL(delay_ms)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_job_t *obj = php_kislayphp_queue_job_from_obj(Z_OBJ_P(getThis()));
    if (obj->finished) {
        RETURN_FALSE;
    }
    bool should_requeue = requeue == nullptr ? true : zend_is_true(requeue);
    zend_long actual_delay = (delay_ms == nullptr || Z_TYPE_P(delay_ms) == IS_NULL) ? -1 : zval_get_long(delay_ms);
    std::string error;
    if (!kislay_queue_client_nack_remote(obj->base_url, obj->id, should_requeue, actual_delay, &error)) {
        zend_throw_exception(zend_ce_exception, error.c_str(), 0);
        return;
    }
    obj->finished = true;
    RETURN_TRUE;
}

PHP_METHOD(KislayPHPQueueJob, release) {
    zval *delay_ms = nullptr;
    ZEND_PARSE_PARAMETERS_START(0, 1)
        Z_PARAM_OPTIONAL
        Z_PARAM_ZVAL(delay_ms)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_job_t *obj = php_kislayphp_queue_job_from_obj(Z_OBJ_P(getThis()));
    if (obj->finished) {
        RETURN_FALSE;
    }
    zend_long actual_delay = (delay_ms == nullptr || Z_TYPE_P(delay_ms) == IS_NULL) ? -1 : zval_get_long(delay_ms);
    std::string error;
    if (!kislay_queue_client_nack_remote(obj->base_url, obj->id, true, actual_delay, &error)) {
        zend_throw_exception(zend_ce_exception, error.c_str(), 0);
        return;
    }
    obj->finished = true;
    RETURN_TRUE;
}

static const zend_function_entry kislayphp_queue_methods[] = {
    PHP_ME(KislayPHPQueue, __construct, arginfo_kislayphp_queue_void, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, setClient, arginfo_kislayphp_queue_set_client, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, enqueue, arginfo_kislayphp_queue_enqueue, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, dequeue, arginfo_kislayphp_queue_dequeue, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, peek, arginfo_kislayphp_queue_dequeue, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, size, arginfo_kislayphp_queue_size, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, clear, arginfo_kislayphp_queue_clear, ZEND_ACC_PUBLIC)
    PHP_FE_END
};

static const zend_function_entry kislayphp_queue_client_interface_methods[] = {
    ZEND_ABSTRACT_ME(KislayPHPQueueClientInterface, enqueue, arginfo_kislayphp_queue_enqueue)
    ZEND_ABSTRACT_ME(KislayPHPQueueClientInterface, dequeue, arginfo_kislayphp_queue_dequeue)
    ZEND_ABSTRACT_ME(KislayPHPQueueClientInterface, size, arginfo_kislayphp_queue_size)
    PHP_FE_END
};

static const zend_function_entry kislayphp_queue_server_methods[] = {
    PHP_ME(KislayPHPQueueServer, __construct, arginfo_kislayphp_queue_server_construct, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueueServer, listen, arginfo_kislayphp_queue_server_listen, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueueServer, run, arginfo_kislayphp_queue_void, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueueServer, stop, arginfo_kislayphp_queue_void, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueueServer, declare, arginfo_kislayphp_queue_server_declare, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueueServer, stats, arginfo_kislayphp_queue_server_stats, ZEND_ACC_PUBLIC)
    PHP_FE_END
};

static const zend_function_entry kislayphp_queue_client_methods[] = {
    PHP_ME(KislayPHPQueueClient, __construct, arginfo_kislayphp_queue_client_construct, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueueClient, push, arginfo_kislayphp_queue_client_push, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueueClient, pushBatch, arginfo_kislayphp_queue_client_push_batch, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueueClient, stats, arginfo_kislayphp_queue_size, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueueClient, purge, arginfo_kislayphp_queue_size, ZEND_ACC_PUBLIC)
    PHP_FE_END
};

static const zend_function_entry kislayphp_queue_worker_methods[] = {
    PHP_ME(KislayPHPQueueWorker, __construct, arginfo_kislayphp_queue_client_construct, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueueWorker, consume, arginfo_kislayphp_queue_worker_consume, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueueWorker, stop, arginfo_kislayphp_queue_void, ZEND_ACC_PUBLIC)
    PHP_FE_END
};

static const zend_function_entry kislayphp_queue_job_methods[] = {
    PHP_ME(KislayPHPQueueJob, id, arginfo_kislayphp_queue_void, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueueJob, queue, arginfo_kislayphp_queue_void, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueueJob, payload, arginfo_kislayphp_queue_void, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueueJob, headers, arginfo_kislayphp_queue_void, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueueJob, attempts, arginfo_kislayphp_queue_void, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueueJob, maxAttempts, arginfo_kislayphp_queue_void, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueueJob, availableAt, arginfo_kislayphp_queue_void, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueueJob, ack, arginfo_kislayphp_queue_void, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueueJob, nack, arginfo_kislayphp_queue_job_nack, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueueJob, release, arginfo_kislayphp_queue_job_nack, ZEND_ACC_PUBLIC)
    PHP_FE_END
};

PHP_MINIT_FUNCTION(kislayphp_queue) {
    zend_class_entry ce;

    INIT_NS_CLASS_ENTRY(ce, "Kislay\\Queue", "ClientInterface", kislayphp_queue_client_interface_methods);
    kislayphp_queue_client_interface_ce = zend_register_internal_interface(&ce);
    zend_register_class_alias("KislayPHP\\Queue\\ClientInterface", kislayphp_queue_client_interface_ce);

    INIT_NS_CLASS_ENTRY(ce, "Kislay\\Queue", "Queue", kislayphp_queue_methods);
    kislayphp_queue_ce = zend_register_internal_class(&ce);
    zend_register_class_alias("KislayPHP\\Queue\\Queue", kislayphp_queue_ce);
    kislayphp_queue_ce->create_object = kislayphp_queue_create_object;
    std::memcpy(&kislayphp_queue_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    kislayphp_queue_handlers.offset = XtOffsetOf(php_kislayphp_queue_t, std);
    kislayphp_queue_handlers.free_obj = kislayphp_queue_free_obj;

    INIT_NS_CLASS_ENTRY(ce, "Kislay\\Queue", "Server", kislayphp_queue_server_methods);
    kislayphp_queue_server_ce = zend_register_internal_class(&ce);
    zend_register_class_alias("KislayPHP\\Queue\\Server", kislayphp_queue_server_ce);
    kislayphp_queue_server_ce->create_object = kislayphp_queue_server_create_object;
    std::memcpy(&kislayphp_queue_server_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    kislayphp_queue_server_handlers.offset = XtOffsetOf(php_kislayphp_queue_server_t, std);
    kislayphp_queue_server_handlers.free_obj = kislayphp_queue_server_free_obj;

    INIT_NS_CLASS_ENTRY(ce, "Kislay\\Queue", "Client", kislayphp_queue_client_methods);
    kislayphp_queue_client_ce = zend_register_internal_class(&ce);
    zend_register_class_alias("KislayPHP\\Queue\\Client", kislayphp_queue_client_ce);
    kislayphp_queue_client_ce->create_object = kislayphp_queue_client_create_object;
    std::memcpy(&kislayphp_queue_client_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    kislayphp_queue_client_handlers.offset = XtOffsetOf(php_kislayphp_queue_client_t, std);
    kislayphp_queue_client_handlers.free_obj = kislayphp_queue_client_free_obj;

    INIT_NS_CLASS_ENTRY(ce, "Kislay\\Queue", "Worker", kislayphp_queue_worker_methods);
    kislayphp_queue_worker_ce = zend_register_internal_class(&ce);
    zend_register_class_alias("KislayPHP\\Queue\\Worker", kislayphp_queue_worker_ce);
    kislayphp_queue_worker_ce->create_object = kislayphp_queue_worker_create_object;
    std::memcpy(&kislayphp_queue_worker_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    kislayphp_queue_worker_handlers.offset = XtOffsetOf(php_kislayphp_queue_worker_t, std);
    kislayphp_queue_worker_handlers.free_obj = kislayphp_queue_worker_free_obj;

    INIT_NS_CLASS_ENTRY(ce, "Kislay\\Queue", "Job", kislayphp_queue_job_methods);
    kislayphp_queue_job_ce = zend_register_internal_class(&ce);
    zend_register_class_alias("KislayPHP\\Queue\\Job", kislayphp_queue_job_ce);
    kislayphp_queue_job_ce->create_object = kislayphp_queue_job_create_object;
    std::memcpy(&kislayphp_queue_job_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    kislayphp_queue_job_handlers.offset = XtOffsetOf(php_kislayphp_queue_job_t, std);
    kislayphp_queue_job_handlers.free_obj = kislayphp_queue_job_free_obj;

    return SUCCESS;
}

PHP_MSHUTDOWN_FUNCTION(kislayphp_queue) {
    return SUCCESS;
}

PHP_MINFO_FUNCTION(kislayphp_queue) {
    php_info_print_table_start();
    php_info_print_table_header(2, "kislayphp_queue support", "enabled");
    php_info_print_table_row(2, "Version", PHP_KISLAYPHP_QUEUE_VERSION);
    php_info_print_table_end();
}

zend_module_entry kislayphp_queue_module_entry = {
    STANDARD_MODULE_HEADER,
    PHP_KISLAYPHP_QUEUE_EXTNAME,
    nullptr,
    PHP_MINIT(kislayphp_queue),
    PHP_MSHUTDOWN(kislayphp_queue),
    nullptr,
    nullptr,
    PHP_MINFO(kislayphp_queue),
    PHP_KISLAYPHP_QUEUE_VERSION,
    STANDARD_MODULE_PROPERTIES
};

#ifdef ZTS
ZEND_TSRMLS_CACHE_DEFINE();
#endif

extern "C" {
ZEND_DLEXPORT zend_module_entry *get_module(void) {
    return &kislayphp_queue_module_entry;
}
}
