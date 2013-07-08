include_directories(${CMAKE_CURRENT_SOURCE_DIR} ../mfscommon)

add_definitions(-DFUSE_USE_VERSION=26)
add_definitions(-DAPPNAME=mfsmount)

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


add_executable(mfsmount/mfsmount ${MOUNT_SRCS})
add_executable(mfsmount/mfstools ${TOOLS_SRCS})

target_link_libraries(mfsmount/mfsmount common ${ZLIB_LIBRARIES} ${FUSE_LIB})
target_link_libraries(mfsmount/mfstools common)

