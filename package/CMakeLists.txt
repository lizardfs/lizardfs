include(set_system_architecture.cmake)

# Add generators
if(BUILD_DEB)
  set(GENERATOR_DEB "DEB")
endif()
if(BUILD_RPM)
  set(GENERATOR_RPM "RPM")
endif()

# Common config
set(CPACK_GENERATOR TGZ ${GENERATOR_DEB} ${GENERATOR_RPM})
set(CPACK_INCLUDE_TOPLEVEL_DIRECTORY FALSE)

set(CPACK_PACKAGE_DESCRIPTION_FILE ${CMAKE_SOURCE_DIR}/README)
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "LizardFS - distributed, fault tolerant file system")
set(CPACK_PACKAGE_NAME "lizardfs")
set(CPACK_PACKAGE_VERSION_MAJOR ${PACKAGE_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${PACKAGE_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_MICRO ${PACKAGE_VERSION_MICRO})
set(CPACK_PACKAGE_VERSION ${PACKAGE_VERSION})
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}_${CPACK_PACKAGE_VERSION}_${SYSTEM_ARCHITECTURE}")
set(CPACK_PACKAGE_VENDOR "http://www.lizardfs.org")
set(CPACK_PACKAGE_CONTACT "contact@lizardfs.org")
set(CPACK_SOURCE_GENERATOR TGZ)

message(STATUS "Generator is ${CPACK_GENERATOR}")

if(BUILD_DEB)
  include(package_deb.cmake)
endif()
if(BUILD_RPM)
  include(package_rpm.cmake)
endif()

#must be the last one
include(CPack)

