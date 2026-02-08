PHP_ARG_ENABLE(kislayphp_queue, whether to enable kislayphp_queue,
[  --enable-kislayphp_queue   Enable kislayphp_queue support])

if test "$PHP_KISLAYPHP_QUEUE" != "no"; then
  PHP_REQUIRE_CXX()
  PHP_NEW_EXTENSION(kislayphp_queue, kislayphp_queue.cpp, $ext_shared)
fi
