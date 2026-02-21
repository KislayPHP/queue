extern "C" {
#include "php.h"
#include "ext/standard/info.h"
#include "Zend/zend_API.h"
#include "Zend/zend_interfaces.h"
#include "Zend/zend_exceptions.h"
}

#include "php_kislayphp_queue.h"

#include <chrono>
#include <cstring>
#include <pthread.h>
#include <cstdlib>
#include <mutex>
#include <string>
#include <unordered_map>
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

typedef struct _php_kislayphp_queue_t {
    std::unordered_map<std::string, std::vector<zval>> queues;
    pthread_mutex_t lock;
    zval client;
    bool has_client;
    zend_object std;
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
    for (auto &entry : obj->queues) {
        for (auto &item : entry.second) {
            zval_ptr_dtor(&item);
        }
    }
    obj->queues.~unordered_map();
    pthread_mutex_destroy(&obj->lock);
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
    ZEND_ARG_OBJ_INFO(0, client, Kislay\\Queue\\ClientInterface, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_kislayphp_queue_clear, 0, 0, 1)
    ZEND_ARG_TYPE_INFO(0, queue, IS_STRING, 0)
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

    zval &item = it->second.front();
    ZVAL_COPY(return_value, &item);
    zval_ptr_dtor(&item);
    it->second.erase(it->second.begin());
    pthread_mutex_unlock(&obj->lock);
    return;
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
    zval &item = it->second.front();
    ZVAL_COPY(return_value, &item);
    pthread_mutex_unlock(&obj->lock);
}

PHP_METHOD(KislayPHPQueue, clear) {
    char *queue = nullptr;
    size_t queue_len = 0;
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

    zend_long removed = 0;
    pthread_mutex_lock(&obj->lock);
    auto it = obj->queues.find(std::string(queue, queue_len));
    if (it != obj->queues.end()) {
        removed = static_cast<zend_long>(it->second.size());
        for (auto &item : it->second) {
            zval_ptr_dtor(&item);
        }
        obj->queues.erase(it);
    }
    pthread_mutex_unlock(&obj->lock);
    RETURN_LONG(removed);
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

static const zend_function_entry kislayphp_queue_client_methods[] = {
    ZEND_ABSTRACT_ME(KislayPHPQueueClientInterface, enqueue, arginfo_kislayphp_queue_enqueue)
    ZEND_ABSTRACT_ME(KislayPHPQueueClientInterface, dequeue, arginfo_kislayphp_queue_dequeue)
    ZEND_ABSTRACT_ME(KislayPHPQueueClientInterface, size, arginfo_kislayphp_queue_size)
    PHP_FE_END
};

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
    kislayphp_queue_handlers.offset = XtOffsetOf(php_kislayphp_queue_t, std);
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

#if defined(COMPILE_DL_KISLAYPHP_QUEUE) || defined(ZEND_COMPILE_DL_EXT)
#ifdef ZTS
ZEND_TSRMLS_CACHE_DEFINE();
#endif
extern "C" {
ZEND_GET_MODULE(kislayphp_queue)
}
#endif
