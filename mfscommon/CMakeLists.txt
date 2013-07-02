include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_library(common cfg.cc charts.cc crc.cc md5.cc pcqueue.cc random.cc sockets.cc strerr.cc)

