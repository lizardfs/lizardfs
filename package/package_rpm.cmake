set(CPACK_RPM_COMPONENT_INSTALL YES)
# Component-wide properties
set(CPACK_RPM_PACKAGE_SUMMARY ${CPACK_PACKAGE_DESCRIPTION_SUMMARY})
set(CPACK_RPM_PACKAGE_NAME ${CPACK_PACKAGE_NAME})
set(CPACK_RPM_PACKAGE_VERSION ${CPACK_PACKAGE_VERSION})
set(CPACK_RPM_PACKAGE_ARCHITECTURE ${SYSTEM_ARCHITECTURE})
set(CPACK_RPM_PACKAGE_RELEASE 1)
set(CPACK_RPM_PACKAGE_LICENSE "GPLv3")
set(CPACK_RPM_PACKAGE_GROUP "System Environment/Daemons")
set(CPACK_RPM_PACKAGE_VENDOR ${CPACK_PACKAGE_VENDOR})
set(CPACK_RPM_PACKAGE_URL "http://www.lizardfs.org")
set(CPACK_RPM_PACKAGE_DESCRIPTION ${CPACK_PACKAGE_DESCRIPTION_FILE})
#set(CPACK_RPM_COMPRESSION_TYPE "gzip") # use system default
#set(CPACK_RPM_PACKAGE_REQUIRES "pkgconfig")

# Defines macro allowing per component basis specifying paths that are
# normally specified per package by CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST.
# Exemplar usage to fix error "file /usr/sbin from install of lizardfs-master-1.6.27-1.i386
# conflicts with file from package filesystem-3.1-2fc18.i686" is
# 'set(CPACK_RPM_master_USER_FILELIST "%ignore /usr/sbin")'
set(CPACK_RPM_SPEC_MORE_DEFINE "%define ignore \#")

# Configure individual components
include(cgi/package_rpm.cmake)
include(cgiserv/package_rpm.cmake)
include(chunkserver/package_rpm.cmake)
include(client/package_rpm.cmake)
include(common/package_rpm.cmake)
include(master/package_rpm.cmake)
include(metalogger/package_rpm.cmake)

