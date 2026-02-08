extern "C" {
#include "php.h"
#include "ext/standard/info.h"
#include "Zend/zend_exceptions.h"
}

#include "php_kislayphp_queue.h"

#include <string>
#include <unordered_map>
#include <vector>

static zend_class_entry *kislayphp_queue_ce;

typedef struct _php_kislayphp_queue_t {
    zend_object std;
    std::unordered_map<std::string, std::vector<zval>> queues;
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
    obj->std.handlers = &kislayphp_queue_handlers;
    return &obj->std;
}

static void kislayphp_queue_free_obj(zend_object *object) {
    php_kislayphp_queue_t *obj = php_kislayphp_queue_from_obj(object);
    for (auto &entry : obj->queues) {
        for (auto &item : entry.second) {
            zval_ptr_dtor(&item);
        }
    }
    obj->queues.~unordered_map();
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

PHP_METHOD(KislayPHPQueue, __construct) {
    ZEND_PARSE_PARAMETERS_NONE();
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
    zval copy;
    ZVAL_COPY(&copy, payload);
    obj->queues[std::string(queue, queue_len)].push_back(copy);
    RETURN_TRUE;
}

PHP_METHOD(KislayPHPQueue, dequeue) {
    char *queue = nullptr;
    size_t queue_len = 0;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(queue, queue_len)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_t *obj = php_kislayphp_queue_from_obj(Z_OBJ_P(getThis()));
    auto it = obj->queues.find(std::string(queue, queue_len));
    if (it == obj->queues.end() || it->second.empty()) {
        RETURN_NULL();
    }

    zval &item = it->second.front();
    ZVAL_COPY(return_value, &item);
    zval_ptr_dtor(&item);
    it->second.erase(it->second.begin());
}

PHP_METHOD(KislayPHPQueue, size) {
    char *queue = nullptr;
    size_t queue_len = 0;
    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(queue, queue_len)
    ZEND_PARSE_PARAMETERS_END();

    php_kislayphp_queue_t *obj = php_kislayphp_queue_from_obj(Z_OBJ_P(getThis()));
    auto it = obj->queues.find(std::string(queue, queue_len));
    if (it == obj->queues.end()) {
        RETURN_LONG(0);
    }
    RETURN_LONG(static_cast<zend_long>(it->second.size()));
}

static const zend_function_entry kislayphp_queue_methods[] = {
    PHP_ME(KislayPHPQueue, __construct, arginfo_kislayphp_queue_void, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, enqueue, arginfo_kislayphp_queue_enqueue, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, dequeue, arginfo_kislayphp_queue_dequeue, ZEND_ACC_PUBLIC)
    PHP_ME(KislayPHPQueue, size, arginfo_kislayphp_queue_size, ZEND_ACC_PUBLIC)
    PHP_FE_END
};

PHP_MINIT_FUNCTION(kislayphp_queue) {
    zend_class_entry ce;
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
