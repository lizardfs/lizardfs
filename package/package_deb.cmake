set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Marcin Sulikowski <contact@lizardfs.org>")
set(CPACK_DEBIAN_PACKAGE_SECTION "admin")
set(CPACK_DEBIAN_PACKAGE_PRIORITY "extra")
set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE ${SYSTEM_ARCHITECTURE})
set(CPACK_DEB_COMPONENT_INSTALL ON)

#set(CPACK_DEBIAN_PACKAGE_DEPENDS "libc6 (>= 2.14), libfuse2 (>= 2.8.1), libgcc1 (>= 1:4.1.1), libstdc++6 (>= 4.1.1)")

#CPack has no support for per-component CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA.
#DebComponentControlExtra.cmake library provides facility to aggregate all scripts
#and select proper one when component is installed
include(DebComponentControlExtra.cmake)
open_deb_component_control_extra("postinst")
open_deb_component_control_extra("prerm")

file(READ "daemon.postinst.in" DAEMON_POSTINST_IN_BODY)
file(READ "daemon.prerm.in" DAEMON_PRERM_IN_BODY)

include(chunkserver/package_deb.cmake)
include(cgi/package_deb.cmake)
include(cgiserv/package_deb.cmake)
include(client/package_deb.cmake)
include(common/package_deb.cmake)
include(master/package_deb.cmake)
include(metalogger/package_deb.cmake)

close_deb_component_control_extra("postinst")
close_deb_component_control_extra("prerm")
set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA "${CMAKE_CURRENT_BINARY_DIR}/prerm;${CMAKE_CURRENT_BINARY_DIR}/postinst")
