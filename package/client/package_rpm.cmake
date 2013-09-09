set(COMPONENT_NAME "client")

set(CPACK_RPM_${COMPONENT_NAME}_PACKAGE_REQUIRES "fuse lizardfs-common")

set(CPACK_RPM_${COMPONENT_NAME}_USER_FILELIST
    "%ignore /usr"        #Provided by filesystem
    "%ignore /usr/bin"
)

