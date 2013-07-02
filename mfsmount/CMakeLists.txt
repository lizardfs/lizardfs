set(MOUNT_SRCS
  chunkloccache.cc
  csdb.cc
  main.cc
  masterproxy.cc
  mfs_meta_fuse.cc
  oplog.cc
  stats.cc
  writedata.cc
  cscomm.cc
  dirattrcache.cc
  mastercomm.cc
  mfs_fuse.cc
  readdata.cc
  symlinkcache.cc)

set(TOOLS_SRCS
  mfstools.cc)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DFUSE_USE_VERSION=26")

add_executable(mfsmount ${MOUNT_SRCS})
add_executable(mfstools ${TOOLS_SRCS})

target_link_libraries(mfsmount common ${M_LIB} ${Z_LIB} ${FUSE_LIB} ${DL_LIB})
target_link_libraries(mfstools common ${M_LIB})

