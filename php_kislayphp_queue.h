#ifndef PHP_KISLAYPHP_QUEUE_H
#define PHP_KISLAYPHP_QUEUE_H

extern "C" {
#include "php.h"
}

#define PHP_KISLAYPHP_QUEUE_VERSION "0.0.2"
#define PHP_KISLAYPHP_QUEUE_EXTNAME "kislayphp_queue"

extern zend_module_entry kislayphp_queue_module_entry;
#define phpext_kislayphp_queue_ptr &kislayphp_queue_module_entry

#endif
