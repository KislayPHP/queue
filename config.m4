PHP_ARG_ENABLE(kislayphp_queue, whether to enable kislayphp_queue,
[  --enable-kislayphp_queue   Enable kislayphp_queue support])

if test "$PHP_KISLAYPHP_QUEUE" != "no"; then
  PHP_REQUIRE_CXX()
  if test -f ../rpc/gen/platform.pb.cc; then
    RPC_GEN_DIR=`pwd`/../rpc/gen
    PHP_ADD_INCLUDE($RPC_GEN_DIR)
    PHP_ADD_INCLUDE(`pwd`/../rpc)
    PKG_CHECK_MODULES([GRPC], [grpc++])
    PHP_EVAL_INCLINE($GRPC_CFLAGS)
    PHP_EVAL_LIBLINE($GRPC_LIBS, KISLAYPHP_QUEUE_SHARED_LIBADD)
    CXXFLAGS="$CXXFLAGS -DKISLAYPHP_RPC"
    RPC_SRCS="../rpc/gen/platform.pb.cc ../rpc/gen/platform.grpc.pb.cc"
  else
    AC_MSG_ERROR([RPC stubs not found. Run ./scripts/generate_rpc_stubs.sh])
  fi

  PHP_NEW_EXTENSION(kislayphp_queue, kislayphp_queue.cpp $RPC_SRCS, $ext_shared)
fi
