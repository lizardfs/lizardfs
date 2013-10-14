add_definitions(-DAPPNAME=mfsmount -DFUSE_USE_VERSION=26)

add_library(mount
	chunk_connector.cc chunk_connector.h
	chunkloccache.cc chunkloccache.h
	cscomm.cc cscomm.h
	csdb.cc csdb.h
	dirattrcache.cc dirattrcache.h
	mastercomm.cc mastercomm.h
	masterproxy.cc masterproxy.h
	mfs_fuse.cc mfs_fuse.h
	mfs_meta_fuse.cc mfs_meta_fuse.h
	oplog.cc oplog.h
	read_operation_executor.cc read_operation_executor.h
	read_operation_planner.cc read_operation_planner.h
	read_plan_executor.cc read_plan_executor.h
	readdata.cc readdata.h
	stats.cc stats.h
	symlinkcache.cc symlinkcache.h
	writedata.cc writedata.h
)
target_link_libraries(mount mfscommon ${M_LIB})

add_executable(mfsmount main.cc)
target_link_libraries(mfsmount mfscommon mount ${M_LIB} ${Z_LIB} ${FUSE_LIB} ${DL_LIB})
install(TARGETS mfsmount RUNTIME DESTINATION bin)
