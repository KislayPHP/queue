extern "C" {
#include "php.h"
#include "ext/standard/info.h"
#include "Zend/zend_API.h"
#include "Zend/zend_interfaces.h"
#include "Zend/zend_exceptions.h"
}

#include "php_kislayphp_queue.h"

#include <cstring>
#include <pthread.h>
#include <string>
#include <unordered_map>
#include <vector>

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
    ZEND_ARG_OBJ_INFO(0, client, KislayPHP\\Queue\\ClientInterface, 0)
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
        zend_throw_exception(zend_ce_exception, "Client must implement KislayPHP\\Queue\\ClientInterface", 0);
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
    INIT_NS_CLASS_ENTRY(ce, "KislayPHP\\Queue", "ClientInterface", kislayphp_queue_client_methods);
    kislayphp_queue_client_ce = zend_register_internal_interface(&ce);
    INIT_NS_CLASS_ENTRY(ce, "KislayPHP\\Queue", "Queue", kislayphp_queue_methods);
    kislayphp_queue_ce = zend_register_internal_class(&ce);
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
