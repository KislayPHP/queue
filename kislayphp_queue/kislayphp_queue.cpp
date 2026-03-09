extern "C" {
#include "php.h"
#include "ext/standard/info.h"
#include "Zend/zend_API.h"
#include "Zend/zend_interfaces.h"
#include "Zend/zend_exceptions.h"
}

#include "php_kislayphp_queue.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <climits>
#include <cstdio>
#include <cstring>
#ifdef _WIN32
  #include <windows.h>
  #ifndef PTHREAD_WIN32_COMPAT
  #define PTHREAD_WIN32_COMPAT
  typedef CRITICAL_SECTION pthread_mutex_t;
  #define pthread_mutex_init(m, a)   InitializeCriticalSection(m)
  #define pthread_mutex_destroy(m)   DeleteCriticalSection(m)
  #define pthread_mutex_lock(m)      EnterCriticalSection(m)
  #define pthread_mutex_unlock(m)    LeaveCriticalSection(m)
  #endif
#else
  #include <pthread.h>
#endif
#include <cstdlib>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "Zend/zend_smart_str.h"
#include "ext/standard/php_var.h"

#ifdef KISLAYPHP_RPC
#include <grpcpp/grpcpp.h>

#include "platform.grpc.pb.h"
#endif

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
static zend_class_entry *kislayphp_queue_client_ce;

static zend_long kislayphp_env_long(const char *name, zend_long fallback) {
    const char *value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        return fallback;
    }
    return static_cast<zend_long>(std::strtoll(value, nullptr, 10));
}

static bool kislayphp_env_bool(const char *name, bool fallback) {
    const char *value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        return fallback;
    }
    if (std::strcmp(value, "1") == 0 || std::strcmp(value, "true") == 0 || std::strcmp(value, "TRUE") == 0) {
        return true;
    }
    if (std::strcmp(value, "0") == 0 || std::strcmp(value, "false") == 0 || std::strcmp(value, "FALSE") == 0) {
        return false;
    }
    return fallback;
}

static std::string kislayphp_env_string(const char *name, const std::string &fallback) {
    const char *value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        return fallback;
    }
    return std::string(value);
}

#ifdef KISLAYPHP_RPC
static bool kislayphp_rpc_enabled() {
    return kislayphp_env_bool("KISLAY_RPC_ENABLED", false);
}

static zend_long kislayphp_rpc_timeout_ms() {
    zend_long timeout = kislayphp_env_long("KISLAY_RPC_TIMEOUT_MS", 200);
    return timeout > 0 ? timeout : 200;
}

static std::string kislayphp_rpc_platform_endpoint() {
    return kislayphp_env_string("KISLAY_RPC_PLATFORM_ENDPOINT", "127.0.0.1:9100");
}

static bool kislayphp_serialize_payload(zval *payload, std::string &out) {
    smart_str buffer = {0};
    php_serialize_data_t var_hash;
    PHP_VAR_SERIALIZE_INIT(var_hash);
    php_var_serialize(&buffer, payload, &var_hash);
    PHP_VAR_SERIALIZE_DESTROY(var_hash);
    if (buffer.s == nullptr) {
        return false;
    }
    out.assign(ZSTR_VAL(buffer.s), ZSTR_LEN(buffer.s));
    smart_str_free(&buffer);
    return true;
}

static bool kislayphp_unserialize_payload(const std::string &data, zval *out) {
    const unsigned char *p = reinterpret_cast<const unsigned char *>(data.data());
    php_unserialize_data_t var_hash;
    PHP_VAR_UNSERIALIZE_INIT(var_hash);
    bool ok = php_var_unserialize(out, &p, p + data.size(), &var_hash);
    PHP_VAR_UNSERIALIZE_DESTROY(var_hash);
    return ok;
}

static kislay::platform::v1::QueueService::Stub *kislayphp_rpc_queue_stub(const std::string &endpoint) {
    static std::mutex lock;
    static std::string cached_endpoint;
    static std::shared_ptr<grpc::Channel> channel;
    static std::unique_ptr<kislay::platform::v1::QueueService::Stub> stub;
    std::lock_guard<std::mutex> guard(lock);
    if (!stub || cached_endpoint != endpoint) {
        channel = grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
        stub = kislay::platform::v1::QueueService::NewStub(channel);
        cached_endpoint = endpoint;
    }
    return stub.get();
}

static bool kislayphp_rpc_queue_enqueue(const std::string &queue, const std::string &payload, std::string *error) {
    auto *stub = kislayphp_rpc_queue_stub(kislayphp_rpc_platform_endpoint());
    if (!stub) {
        if (error) {
            *error = "RPC stub unavailable";
        }
        return false;
    }

    kislay::platform::v1::EnqueueRequest request;
    request.set_queue(queue);
    request.set_payload(payload);
    kislay::platform::v1::EnqueueResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(kislayphp_rpc_timeout_ms()));

    grpc::Status status = stub->Enqueue(&context, request, &response);
    if (!status.ok()) {
        if (error) {
            *error = status.error_message();
        }
        return false;
    }
    if (!response.ok()) {
        if (error) {
            *error = response.error();
        }
        return false;
    }
    return true;
}

static bool kislayphp_rpc_queue_dequeue(const std::string &queue, std::string *payload, bool *ok, std::string *error) {
    auto *stub = kislayphp_rpc_queue_stub(kislayphp_rpc_platform_endpoint());
    if (!stub) {
        if (error) {
            *error = "RPC stub unavailable";
        }
        return false;
    }

    kislay::platform::v1::DequeueRequest request;
    request.set_queue(queue);
    kislay::platform::v1::DequeueResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(kislayphp_rpc_timeout_ms()));

    grpc::Status status = stub->Dequeue(&context, request, &response);
    if (!status.ok()) {
        if (error) {
            *error = status.error_message();
        }
        return false;
    }
    if (ok) {
        *ok = response.ok();
    }
    if (payload) {
        *payload = response.payload();
    }
    return true;
}

static bool kislayphp_rpc_queue_peek(const std::string &queue, std::string *payload, bool *ok, std::string *error) {
    auto *stub = kislayphp_rpc_queue_stub(kislayphp_rpc_platform_endpoint());
    if (!stub) {
        if (error) {
            *error = "RPC stub unavailable";
        }
        return false;
    }

    kislay::platform::v1::PeekRequest request;
    request.set_queue(queue);
    kislay::platform::v1::PeekResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(kislayphp_rpc_timeout_ms()));

    grpc::Status status = stub->Peek(&context, request, &response);
    if (!status.ok()) {
        if (error) {
            *error = status.error_message();
        }
        return false;
    }
    if (ok) {
        *ok = response.ok();
    }
    if (payload) {
        *payload = response.payload();
    }
    return true;
}

static bool kislayphp_rpc_queue_size(const std::string &queue, zend_long *size, std::string *error) {
    auto *stub = kislayphp_rpc_queue_stub(kislayphp_rpc_platform_endpoint());
    if (!stub) {
        if (error) {
            *error = "RPC stub unavailable";
        }
        return false;
    }

    kislay::platform::v1::SizeRequest request;
    request.set_queue(queue);
    kislay::platform::v1::SizeResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(kislayphp_rpc_timeout_ms()));

    grpc::Status status = stub->Size(&context, request, &response);
    if (!status.ok()) {
        if (error) {
            *error = status.error_message();
        }
        return false;
    }
    if (size) {
        *size = static_cast<zend_long>(response.size());
    }
    return true;
}

static bool kislayphp_rpc_queue_clear(const std::string &queue, zend_long *removed, std::string *error) {
    auto *stub = kislayphp_rpc_queue_stub(kislayphp_rpc_platform_endpoint());
    if (!stub) {
        if (error) {
            *error = "RPC stub unavailable";
        }
        return false;
    }

    kislay::platform::v1::ClearRequest request;
    request.set_queue(queue);
    kislay::platform::v1::ClearResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(kislayphp_rpc_timeout_ms()));

    grpc::Status status = stub->Clear(&context, request, &response);
    if (!status.ok()) {
        if (error) {
            *error = status.error_message();
        }
        return false;
    }
    if (removed) {
        *removed = static_cast<zend_long>(response.removed());
    }
    return true;
}
#endif

/* -----------------------------------------------------------------------
 * Helpers
 * --------------------------------------------------------------------- */

static int64_t kislayphp_now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

static std::atomic<uint64_t> kislayphp_msg_seq{0};

static std::string kislayphp_generate_message_id() {
    uint64_t id = kislayphp_msg_seq.fetch_add(1, std::memory_order_relaxed);
    char buf[17];
    std::snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(id));
    return std::string(buf);
}

/* -----------------------------------------------------------------------
 * Queue item: holds payload + scheduling / routing metadata.
 *
 * zval lifetime is managed explicitly:
 *   - copy-ctor calls ZVAL_COPY  (increments refcount)
 *   - move-ctor transfers bytes  and sets source to IS_UNDEF
 *   - dtor calls zval_ptr_dtor   (UNDEF is a safe no-op)
 * --------------------------------------------------------------------- */

struct kislayphp_queue_item {
    zval     value;
    int      priority;
    int64_t  enqueue_time_ms;
    int64_t  ttl_ms;           /* 0 = no expiry */
    int64_t  delay_until_ms;   /* 0 = immediate; epoch-ms after which available */
    std::string message_id;

    kislayphp_queue_item()
        : priority(0), enqueue_time_ms(0), ttl_ms(0), delay_until_ms(0) {
        ZVAL_UNDEF(&value);
    }

    ~kislayphp_queue_item() {
        zval_ptr_dtor(&value);
    }

    kislayphp_queue_item(const kislayphp_queue_item &o)
        : priority(o.priority), enqueue_time_ms(o.enqueue_time_ms),
          ttl_ms(o.ttl_ms), delay_until_ms(o.delay_until_ms),
          message_id(o.message_id) {
        ZVAL_COPY(&value, const_cast<zval *>(&o.value));
    }

    kislayphp_queue_item &operator=(const kislayphp_queue_item &o) {
        if (this != &o) {
            zval_ptr_dtor(&value);
            ZVAL_COPY(&value, const_cast<zval *>(&o.value));
            priority         = o.priority;
            enqueue_time_ms  = o.enqueue_time_ms;
            ttl_ms           = o.ttl_ms;
            delay_until_ms   = o.delay_until_ms;
            message_id       = o.message_id;
        }
        return *this;
    }

    kislayphp_queue_item(kislayphp_queue_item &&o) noexcept
        : priority(o.priority), enqueue_time_ms(o.enqueue_time_ms),
          ttl_ms(o.ttl_ms), delay_until_ms(o.delay_until_ms),
          message_id(std::move(o.message_id)) {
        value = o.value;
        ZVAL_UNDEF(&o.value);
    }

    kislayphp_queue_item &operator=(kislayphp_queue_item &&o) noexcept {
        if (this != &o) {
            zval_ptr_dtor(&value);
            value            = o.value;
            ZVAL_UNDEF(&o.value);
            priority         = o.priority;
            enqueue_time_ms  = o.enqueue_time_ms;
            ttl_ms           = o.ttl_ms;
            delay_until_ms   = o.delay_until_ms;
            message_id       = std::move(o.message_id);
        }
        return *this;
    }
};

/* Entry stored in the in-flight map while waiting for ack/nack. */
struct kislayphp_inflight_entry {
    std::string          queue_name;
    kislayphp_queue_item item;

    kislayphp_inflight_entry() = default;

    kislayphp_inflight_entry(kislayphp_inflight_entry &&o) noexcept
        : queue_name(std::move(o.queue_name)), item(std::move(o.item)) {}

    kislayphp_inflight_entry &operator=(kislayphp_inflight_entry &&o) noexcept {
        if (this != &o) {
            queue_name = std::move(o.queue_name);
            item       = std::move(o.item);
        }
        return *this;
    }

    kislayphp_inflight_entry(const kislayphp_inflight_entry &) = delete;
    kislayphp_inflight_entry &operator=(const kislayphp_inflight_entry &) = delete;
};

/* -----------------------------------------------------------------------
 * Per-object state
 * --------------------------------------------------------------------- */

typedef struct _php_kislayphp_queue_t {
    /* key → ordered list of queue items (highest-priority first via dequeue logic) */
    std::unordered_map<std::string, std::vector<kislayphp_queue_item>> queues;

    /* source queue name → DLQ name */
    std::unordered_map<std::string, std::string> dlq_map;

    /* message_id → {queue_name, item} for items dequeued but not yet ack'd */
    std::unordered_map<std::string, kislayphp_inflight_entry> in_flight;

    /* queue name → list of subscriber callables */
    std::unordered_map<std::string, std::vector<zval>> subscribers;

    pthread_mutex_t lock;
    zval            client;
    bool            has_client;
    zend_object     std;
} php_kislayphp_queue_t;

static zend_object_handlers kislayphp_queue_handlers;

static inline php_kislayphp_queue_t *php_kislayphp_queue_from_obj(zend_object *obj) {
    return reinterpret_cast<php_kislayphp_queue_t *>(
        reinterpret_cast<char *>(obj) - XtOffsetOf(php_kislayphp_queue_t, std));
}

static zend_object *kislayphp_queue_create_object(zend_class_entry *ce) {
    php_kislayphp_queue_t *obj = static_cast<php_kislayphp_queue_t *>(
        ecalloc(1, sizeof(php_kislayphp_queue_t) + zend_object_properties_size(ce)));
    zend_object_std_init(&obj->std, ce);
    object_properties_init(&obj->std, ce);
    new (&obj->queues)      std::unordered_map<std::string, std::vector<kislayphp_queue_item>>();
    new (&obj->dlq_map)     std::unordered_map<std::string, std::string>();
    new (&obj->in_flight)   std::unordered_map<std::string, kislayphp_inflight_entry>();
    new (&obj->subscribers) std::unordered_map<std::string, std::vector<zval>>();
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
    /* kislayphp_queue_item destructor handles zval_ptr_dtor for queued items */
    obj->queues.~unordered_map();
    /* in-flight items also cleaned up via destructor */
    obj->in_flight.~unordered_map();
    obj->dlq_map.~unordered_map();
    /* subscriber callables are raw zvals; free manually before map destructs */
    for (auto &entry : obj->subscribers) {
        for (auto &cb : entry.second) {
            zval_ptr_dtor(&cb);
        }
    }
    obj->subscribers.~unordered_map();
    pthread_mutex_destroy(&obj->lock);
    zend_object_std_dtor(&obj->std);
}

/* -----------------------------------------------------------------------
 * Internal helpers
 * --------------------------------------------------------------------- */

/*
 * Find the index of the best candidate item in `items`:
 *   - not expired (ttl_ms == 0 OR elapsed <= ttl_ms)
 *   - not delayed  (delay_until_ms == 0 OR now >= delay_until_ms)
 *   - highest priority; ties broken by FIFO (earlier index wins)
 * Returns -1 if no eligible item exists.
 */
static int kislayphp_find_best(
    const std::vector<kislayphp_queue_item> &items,
    int64_t now)
{
    int best_idx      = -1;
    int best_priority = INT_MIN;

    for (int i = 0, n = static_cast<int>(items.size()); i < n; ++i) {
        const kislayphp_queue_item &item = items[static_cast<size_t>(i)];

        /* skip expired */
        if (item.ttl_ms > 0 && (now - item.enqueue_time_ms) > item.ttl_ms) {
            continue;
        }
        /* skip delayed */
        if (item.delay_until_ms > 0 && now < item.delay_until_ms) {
            continue;
        }
        /* first eligible, or strictly higher priority (FIFO within same priority) */
        if (best_idx == -1 || item.priority > best_priority) {
            best_idx      = i;
            best_priority = item.priority;
        }
    }
    return best_idx;
}

/* -----------------------------------------------------------------------
 * arginfo
 * --------------------------------------------------------------------- */

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_void, 0, 0, 0)
ZEND_END_ARG_INFO()

/* enqueue(string $queue, mixed $payload,
 *         int $ttlMs = 0, int $priority = 0, int $delayMs = 0): bool */
ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_enqueue, 0, 0, 2)
    ZEND_ARG_TYPE_INFO(0, queue, IS_STRING, 0)
    ZEND_ARG_INFO(0, payload)
    ZEND_ARG_TYPE_INFO(0, ttlMs, IS_LONG, 0)
    ZEND_ARG_TYPE_INFO(0, priority, IS_LONG, 0)
    ZEND_ARG_TYPE_INFO(0, delayMs, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_dequeue, 0, 0, 1)
    ZEND_ARG_TYPE_INFO(0, queue, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_size, 0, 0, 1)
    ZEND_ARG_TYPE_INFO(0, queue, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_set_client, 0, 0, 1)
    ZEND_ARG_OBJ_INFO(0, client, Kislay\\Queue\\ClientInterface, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_clear, 0, 0, 1)
    ZEND_ARG_TYPE_INFO(0, queue, IS_STRING, 0)
ZEND_END_ARG_INFO()

/* setDLQ(string $sourceName, string $dlqName): void */
ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_set_dlq, 0, 0, 2)
    ZEND_ARG_TYPE_INFO(0, sourceName, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, dlqName, IS_STRING, 0)
ZEND_END_ARG_INFO()

/* ack(string $messageId): bool */
ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_ack, 0, 0, 1)
    ZEND_ARG_TYPE_INFO(0, messageId, IS_STRING, 0)
ZEND_END_ARG_INFO()

/* nack(string $messageId, bool $requeue = true): bool */
ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_nack, 0, 0, 1)
    ZEND_ARG_TYPE_INFO(0, messageId, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, requeue, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

/* purgeExpired(string $name): int */
ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_purge_expired, 0, 0, 1)
    ZEND_ARG_TYPE_INFO(0, name, IS_STRING, 0)
ZEND_END_ARG_INFO()

/* subscribe(string $name, callable $fn): void */
ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_subscribe, 0, 0, 2)
    ZEND_ARG_TYPE_INFO(0, name, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, fn, IS_CALLABLE, 0)
ZEND_END_ARG_INFO()

/* -----------------------------------------------------------------------
 * PHP methods
 * --------------------------------------------------------------------- */

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

    if (!instanceof_function(Z_OBJCE_P(client), kislayphp_queue_client_ce)) {
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

/*
 * enqueue(string $queue, mixed $payload,
 *         int $ttlMs = 0, int $priority = 0, int $delayMs = 0): bool
 *
 * ttlMs    – item expires after this many milliseconds (0 = never)
 * priority – higher value is dequeued first (0 = default)
 * delayMs  – item becomes available after this many milliseconds (0 = now)
 *
 * When a client or RPC transport is configured the extra scheduling
 * parameters are forwarded as-is to the transport; local scheduling
 * semantics apply only to the in-process store.
 */
PHP_METHOD(KislayPHPQueue, enqueue) {
    char   *queue     = nullptr;
    size_t  queue_len = 0;
    zval   *payload   = nullptr;
    zend_long ttl_ms  = 0;
    zend_long priority = 0;
    zend_long delay_ms = 0;

    ZEND_PARSE_PARAMETERS_START(2, 5)
        Z_PARAM_STRING(queue, queue_len)
        Z_PARAM_ZVAL(payload)
        Z_PARAM_OPTIONAL
        Z_PARAM_LONG(ttl_ms)
        Z_PARAM_LONG(priority)
        Z_PARAM_LONG(delay_ms)
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

#ifdef KISLAYPHP_RPC
    if (kislayphp_rpc_enabled()) {
        std::string payload_bytes;
        if (kislayphp_serialize_payload(payload, payload_bytes)) {
            std::string error;
            if (kislayphp_rpc_queue_enqueue(std::string(queue, queue_len), payload_bytes, &error)) {
                RETURN_TRUE;
            }
        }
    }
#endif

    int64_t now = kislayphp_now_ms();
    std::string qname(queue, queue_len);
    std::string msg_id = kislayphp_generate_message_id();

    kislayphp_queue_item new_item;
    ZVAL_COPY(&new_item.value, payload);
    new_item.priority        = static_cast<int>(priority);
    new_item.enqueue_time_ms = now;
    new_item.ttl_ms          = (ttl_ms > 0) ? ttl_ms : 0;
    new_item.delay_until_ms  = (delay_ms > 0) ? (now + delay_ms) : 0;
    new_item.message_id      = msg_id;

    /* Collect subscriber callbacks under the lock, call them outside. */
    std::vector<zval> cbs_to_call;

    pthread_mutex_lock(&obj->lock);
    obj->queues[qname].push_back(std::move(new_item));
    auto sub_it = obj->subscribers.find(qname);
    if (sub_it != obj->subscribers.end()) {
        for (const auto &cb : sub_it->second) {
            zval cb_copy;
            ZVAL_COPY(&cb_copy, const_cast<zval *>(&cb));
            cbs_to_call.push_back(cb_copy);
        }
    }
    pthread_mutex_unlock(&obj->lock);

    /* Invoke subscribers synchronously outside the lock to avoid deadlock. */
    if (!cbs_to_call.empty()) {
        zval mid_zv;
        ZVAL_STRING(&mid_zv, msg_id.c_str());
        for (auto &cb : cbs_to_call) {
            zval retval_cb;
            ZVAL_UNDEF(&retval_cb);
            zval params[2];
            ZVAL_COPY(&params[0], payload);
            ZVAL_COPY_VALUE(&params[1], &mid_zv);
            call_user_function(nullptr, nullptr, &cb, &retval_cb, 2, params);
            zval_ptr_dtor(&params[0]);
            if (!Z_ISUNDEF(retval_cb)) {
                zval_ptr_dtor(&retval_cb);
            }
            zval_ptr_dtor(&cb);
        }
        zval_ptr_dtor(&mid_zv);
    }

    RETURN_TRUE;
}

/*
 * dequeue(string $queue): mixed
 *
 * Backwards-compatible dequeue: removes and returns the highest-priority
 * eligible item (respects TTL and delay).  Returns null when the queue is
 * empty or all items are delayed/expired.
 *
 * Items removed here do NOT enter the in-flight map; use dequeueWithId()
 * for the ack/nack workflow.
 */
PHP_METHOD(KislayPHPQueue, dequeue) {
    char   *queue     = nullptr;
    size_t  queue_len = 0;
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

#ifdef KISLAYPHP_RPC
    if (kislayphp_rpc_enabled()) {
        std::string payload_bytes;
        bool ok = false;
        std::string error;
        if (kislayphp_rpc_queue_dequeue(std::string(queue, queue_len), &payload_bytes, &ok, &error)) {
            if (!ok) {
                RETURN_NULL();
            }
            if (kislayphp_unserialize_payload(payload_bytes, return_value)) {
                return;
            }
        }
    }
#endif

    pthread_mutex_lock(&obj->lock);
    auto it = obj->queues.find(std::string(queue, queue_len));
    if (it == obj->queues.end() || it->second.empty()) {
        pthread_mutex_unlock(&obj->lock);
        RETURN_NULL();
    }

    int64_t now = kislayphp_now_ms();
    int best_idx = kislayphp_find_best(it->second, now);
    if (best_idx == -1) {
        pthread_mutex_unlock(&obj->lock);
        RETURN_NULL();
    }

    auto &items = it->second;
    /* Transfer zval ownership to return_value without touching the refcount. */
    ZVAL_COPY_VALUE(return_value, &items[static_cast<size_t>(best_idx)].value);
    ZVAL_UNDEF(&items[static_cast<size_t>(best_idx)].value);
    items.erase(items.begin() + best_idx);
    pthread_mutex_unlock(&obj->lock);
}

/*
 * dequeueWithId(string $queue): ?array
 *
 * Removes the best eligible item from the queue and moves it to the
 * in-flight map.  Returns ['value' => mixed, 'id' => string] so the
 * caller can later call ack() or nack().  Returns null when no eligible
 * item is available.
 */
PHP_METHOD(KislayPHPQueue, dequeueWithId) {
    char   *queue     = nullptr;
    size_t  queue_len = 0;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(queue, queue_len)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_t *obj = php_kislayphp_queue_from_obj(Z_OBJ_P(getThis()));

    pthread_mutex_lock(&obj->lock);
    std::string qname(queue, queue_len);
    auto it = obj->queues.find(qname);
    if (it == obj->queues.end() || it->second.empty()) {
        pthread_mutex_unlock(&obj->lock);
        RETURN_NULL();
    }

    int64_t now      = kislayphp_now_ms();
    int best_idx     = kislayphp_find_best(it->second, now);
    if (best_idx == -1) {
        pthread_mutex_unlock(&obj->lock);
        RETURN_NULL();
    }

    auto &items = it->second;

    /*
     * Build the return array before modifying the queue so we can ZVAL_COPY
     * the value safely.  Then transfer ownership to in_flight.
     */
    std::string the_msg_id = items[static_cast<size_t>(best_idx)].message_id;

    array_init(return_value);
    zval value_copy;
    ZVAL_COPY(&value_copy, &items[static_cast<size_t>(best_idx)].value);
    add_assoc_zval(return_value, "value", &value_copy);   /* array steals value_copy */
    add_assoc_string(return_value, "id", the_msg_id.c_str());

    /* Move item from queue into in_flight. */
    kislayphp_inflight_entry entry;
    entry.queue_name = qname;
    entry.item       = std::move(items[static_cast<size_t>(best_idx)]);

    items.erase(items.begin() + best_idx);  /* moved-from item has UNDEF zval */
    obj->in_flight[the_msg_id] = std::move(entry);

    pthread_mutex_unlock(&obj->lock);
}

/*
 * ack(string $messageId): bool
 *
 * Confirms that the in-flight item identified by $messageId has been
 * processed successfully.  The item is discarded.
 */
PHP_METHOD(KislayPHPQueue, ack) {
    char   *msg_id     = nullptr;
    size_t  msg_id_len = 0;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(msg_id, msg_id_len)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_t *obj = php_kislayphp_queue_from_obj(Z_OBJ_P(getThis()));
    std::string mid(msg_id, msg_id_len);

    pthread_mutex_lock(&obj->lock);
    auto it = obj->in_flight.find(mid);
    if (it == obj->in_flight.end()) {
        pthread_mutex_unlock(&obj->lock);
        RETURN_FALSE;
    }
    /* Erasing the entry calls kislayphp_queue_item::~kislayphp_queue_item()
     * which runs zval_ptr_dtor on the stored value. */
    obj->in_flight.erase(it);
    pthread_mutex_unlock(&obj->lock);
    RETURN_TRUE;
}

/*
 * nack(string $messageId, bool $requeue = true): bool
 *
 * Signals that processing of the in-flight item failed.
 *   requeue = true  → item is returned to its source queue (delay reset
 *                     to 0; priority and TTL are preserved).
 *   requeue = false → item is discarded or routed to the DLQ if one has
 *                     been configured via setDLQ().
 */
PHP_METHOD(KislayPHPQueue, nack) {
    char      *msg_id     = nullptr;
    size_t     msg_id_len = 0;
    zend_bool  requeue    = 1;

    ZEND_PARSE_PARAMETERS_START(1, 2)
        Z_PARAM_STRING(msg_id, msg_id_len)
        Z_PARAM_OPTIONAL
        Z_PARAM_BOOL(requeue)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_t *obj = php_kislayphp_queue_from_obj(Z_OBJ_P(getThis()));
    std::string mid(msg_id, msg_id_len);

    pthread_mutex_lock(&obj->lock);
    auto it = obj->in_flight.find(mid);
    if (it == obj->in_flight.end()) {
        pthread_mutex_unlock(&obj->lock);
        RETURN_FALSE;
    }

    if (requeue) {
        std::string qname              = std::move(it->second.queue_name);
        kislayphp_queue_item restored  = std::move(it->second.item);
        restored.delay_until_ms        = 0;  /* deliver immediately */
        obj->in_flight.erase(it);
        obj->queues[qname].push_back(std::move(restored));
    } else {
        auto dlq_it = obj->dlq_map.find(it->second.queue_name);
        if (dlq_it != obj->dlq_map.end()) {
            /* Route to the dead-letter queue. */
            kislayphp_queue_item dlq_item;
            ZVAL_COPY(&dlq_item.value, &it->second.item.value);
            dlq_item.priority        = it->second.item.priority;
            dlq_item.enqueue_time_ms = kislayphp_now_ms();
            dlq_item.ttl_ms          = 0;
            dlq_item.delay_until_ms  = 0;
            dlq_item.message_id      = kislayphp_generate_message_id();
            obj->queues[dlq_it->second].push_back(std::move(dlq_item));
        }
        /* Item destructor frees the zval when erased. */
        obj->in_flight.erase(it);
    }

    pthread_mutex_unlock(&obj->lock);
    RETURN_TRUE;
}

/*
 * setDLQ(string $sourceName, string $dlqName): void
 *
 * Configures a dead-letter queue for $sourceName.  Items nack'd with
 * requeue=false from $sourceName will be enqueued into $dlqName.
 */
PHP_METHOD(KislayPHPQueue, setDLQ) {
    char   *src     = nullptr;
    size_t  src_len = 0;
    char   *dlq     = nullptr;
    size_t  dlq_len = 0;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(src, src_len)
        Z_PARAM_STRING(dlq, dlq_len)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_t *obj = php_kislayphp_queue_from_obj(Z_OBJ_P(getThis()));

    pthread_mutex_lock(&obj->lock);
    obj->dlq_map[std::string(src, src_len)] = std::string(dlq, dlq_len);
    pthread_mutex_unlock(&obj->lock);
}

/*
 * purgeExpired(string $name): int
 *
 * Removes all TTL-expired items from the named queue.
 * Returns the number of items removed.
 */
PHP_METHOD(KislayPHPQueue, purgeExpired) {
    char   *queue     = nullptr;
    size_t  queue_len = 0;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(queue, queue_len)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_t *obj = php_kislayphp_queue_from_obj(Z_OBJ_P(getThis()));
    int64_t now = kislayphp_now_ms();
    zend_long purged = 0;

    pthread_mutex_lock(&obj->lock);
    auto it = obj->queues.find(std::string(queue, queue_len));
    if (it != obj->queues.end()) {
        auto &items = it->second;
        size_t before = items.size();
        /*
         * std::remove_if with move-assignment shifts eligible items to the
         * front.  The destructor on erased elements calls zval_ptr_dtor for
         * any remaining zval (expired items not overwritten keep their zval;
         * those overwritten via move-assignment have UNDEF after the move).
         */
        auto new_end = std::remove_if(
            items.begin(), items.end(),
            [now](const kislayphp_queue_item &item) {
                return item.ttl_ms > 0 && (now - item.enqueue_time_ms) > item.ttl_ms;
            });
        items.erase(new_end, items.end());
        purged = static_cast<zend_long>(before - items.size());
    }
    pthread_mutex_unlock(&obj->lock);
    RETURN_LONG(purged);
}

/*
 * subscribe(string $name, callable $fn): void
 *
 * Registers a push callback for $name.  Every time an item is enqueued
 * (in-process path only) the callback is invoked synchronously:
 *   fn(mixed $value, string $messageId): void
 *
 * Multiple subscribers can be registered for the same queue.
 */
PHP_METHOD(KislayPHPQueue, subscribe) {
    char   *queue     = nullptr;
    size_t  queue_len = 0;
    zval   *fn        = nullptr;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(queue, queue_len)
        Z_PARAM_ZVAL(fn)
    ZEND_PARSE_PARAMETERS_END();

    if (!zend_is_callable(fn, 0, nullptr)) {
        zend_throw_exception(zend_ce_exception, "Argument 2 must be callable", 0);
        return;
    }

    php_kislayphp_queue_t *obj = php_kislayphp_queue_from_obj(Z_OBJ_P(getThis()));

    zval cb_copy;
    ZVAL_COPY(&cb_copy, fn);

    pthread_mutex_lock(&obj->lock);
    obj->subscribers[std::string(queue, queue_len)].push_back(cb_copy);
    pthread_mutex_unlock(&obj->lock);
}

PHP_METHOD(KislayPHPQueue, size) {
    char   *queue     = nullptr;
    size_t  queue_len = 0;
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

#ifdef KISLAYPHP_RPC
    if (kislayphp_rpc_enabled()) {
        zend_long size = 0;
        std::string error;
        if (kislayphp_rpc_queue_size(std::string(queue, queue_len), &size, &error)) {
            RETURN_LONG(size);
        }
    }
#endif

    pthread_mutex_lock(&obj->lock);
    auto it = obj->queues.find(std::string(queue, queue_len));
    if (it == obj->queues.end()) {
        pthread_mutex_unlock(&obj->lock);
        RETURN_LONG(0);
    }
    zend_long count = static_cast<zend_long>(it->second.size());
    pthread_mutex_unlock(&obj->lock);
    RETURN_LONG(count);
}

PHP_METHOD(KislayPHPQueue, peek) {
    char   *queue     = nullptr;
    size_t  queue_len = 0;
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

#ifdef KISLAYPHP_RPC
    if (kislayphp_rpc_enabled()) {
        std::string payload_bytes;
        bool ok = false;
        std::string error;
        if (kislayphp_rpc_queue_peek(std::string(queue, queue_len), &payload_bytes, &ok, &error)) {
            if (!ok) {
                RETURN_NULL();
            }
            if (kislayphp_unserialize_payload(payload_bytes, return_value)) {
                return;
            }
        }
    }
#endif

    pthread_mutex_lock(&obj->lock);
    auto it = obj->queues.find(std::string(queue, queue_len));
    if (it == obj->queues.end() || it->second.empty()) {
        pthread_mutex_unlock(&obj->lock);
        RETURN_NULL();
    }
    int64_t now      = kislayphp_now_ms();
    int best_idx     = kislayphp_find_best(it->second, now);
    if (best_idx == -1) {
        pthread_mutex_unlock(&obj->lock);
        RETURN_NULL();
    }
    ZVAL_COPY(return_value, &it->second[static_cast<size_t>(best_idx)].value);
    pthread_mutex_unlock(&obj->lock);
}

PHP_METHOD(KislayPHPQueue, clear) {
    char   *queue     = nullptr;
    size_t  queue_len = 0;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(queue, queue_len)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_t *obj = php_kislayphp_queue_from_obj(Z_OBJ_P(getThis()));

    if (obj->has_client) {
        zval queue_zv;
        ZVAL_STRINGL(&queue_zv, queue, queue_len);
        zval count_zv;
        ZVAL_LONG(&count_zv, 0);
        zval retval;
        ZVAL_UNDEF(&retval);
        zend_call_method_with_2_params(Z_OBJ(obj->client), Z_OBJCE(obj->client), nullptr, "enqueue", &retval, &queue_zv, &count_zv);
        zval_ptr_dtor(&queue_zv);
        if (!Z_ISUNDEF(retval)) {
            zval_ptr_dtor(&retval);
        }
        RETURN_TRUE;
    }

#ifdef KISLAYPHP_RPC
    if (kislayphp_rpc_enabled()) {
        zend_long removed = 0;
        std::string error;
        if (kislayphp_rpc_queue_clear(std::string(queue, queue_len), &removed, &error)) {
            RETURN_LONG(removed);
        }
    }
#endif

    pthread_mutex_lock(&obj->lock);
    auto it = obj->queues.find(std::string(queue, queue_len));
    if (it == obj->queues.end()) {
        pthread_mutex_unlock(&obj->lock);
        RETURN_LONG(0);
    }
    zend_long removed = static_cast<zend_long>(it->second.size());
    /* vector destructor calls kislayphp_queue_item::~kislayphp_queue_item()
     * for each element, which runs zval_ptr_dtor. */
    obj->queues.erase(it);
    pthread_mutex_unlock(&obj->lock);
    RETURN_LONG(removed);
}

/* -----------------------------------------------------------------------
 * Method tables
 * --------------------------------------------------------------------- */

static const zend_function_entry kislayphp_queue_methods[] = {
    PHP_ME(KislayPHPQueue, __construct,   arginfo_kislayphp_queue_void,         ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, setClient,     arginfo_kislayphp_queue_set_client,   ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, enqueue,       arginfo_kislayphp_queue_enqueue,      ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, dequeue,       arginfo_kislayphp_queue_dequeue,      ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, dequeueWithId, arginfo_kislayphp_queue_dequeue,      ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, ack,           arginfo_kislayphp_queue_ack,          ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, nack,          arginfo_kislayphp_queue_nack,         ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, setDLQ,        arginfo_kislayphp_queue_set_dlq,      ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, purgeExpired,  arginfo_kislayphp_queue_purge_expired, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, subscribe,     arginfo_kislayphp_queue_subscribe,    ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, peek,          arginfo_kislayphp_queue_dequeue,      ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, size,          arginfo_kislayphp_queue_size,         ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, clear,         arginfo_kislayphp_queue_clear,        ZEND_ACC_PUBLIC)
    PHP_FE_END
};

static const zend_function_entry kislayphp_queue_client_methods[] = {
    ZEND_ABSTRACT_ME(KislayPHPQueueClientInterface, enqueue, arginfo_kislayphp_queue_enqueue)
    ZEND_ABSTRACT_ME(KislayPHPQueueClientInterface, dequeue, arginfo_kislayphp_queue_dequeue)
    ZEND_ABSTRACT_ME(KislayPHPQueueClientInterface, size,    arginfo_kislayphp_queue_size)
    PHP_FE_END
};

/* -----------------------------------------------------------------------
 * Extension lifecycle
 * --------------------------------------------------------------------- */

PHP_MINIT_FUNCTION(kislayphp_queue) {
    zend_class_entry ce;
    INIT_NS_CLASS_ENTRY(ce, "Kislay\\Queue", "ClientInterface", kislayphp_queue_client_methods);
    kislayphp_queue_client_ce = zend_register_internal_interface(&ce);
    zend_register_class_alias("KislayPHP\\Queue\\ClientInterface", kislayphp_queue_client_ce);

    INIT_NS_CLASS_ENTRY(ce, "Kislay\\Queue", "Queue", kislayphp_queue_methods);
    kislayphp_queue_ce = zend_register_internal_class(&ce);
    zend_register_class_alias("KislayPHP\\Queue\\Queue", kislayphp_queue_ce);
    kislayphp_queue_ce->create_object = kislayphp_queue_create_object;
    std::memcpy(&kislayphp_queue_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    kislayphp_queue_handlers.offset   = XtOffsetOf(php_kislayphp_queue_t, std);
    kislayphp_queue_handlers.free_obj = kislayphp_queue_free_obj;
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
