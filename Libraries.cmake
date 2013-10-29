find_package(ZLIB REQUIRED)
find_package(Socket)
find_package(Threads)

find_library(FUSE_LIBRARY fuse)
message(STATUS "FUSE_LIBRARY: ${FUSE_LIBRARY}")
