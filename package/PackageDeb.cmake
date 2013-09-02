set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Marcin Sulikowski <contact@lizardfs.org>")
set(CPACK_DEBIAN_PACKAGE_SECTION "admin")
set(CPACK_DEBIAN_PACKAGE_PRIORITY "extra")
set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE ${SYSTEM_ARCHITECTURE})

#set(CPACK_DEBIAN_PACKAGE_DEPENDS "libc6 (>= 2.14), libfuse2 (>= 2.8.1), libgcc1 (>= 1:4.1.1), libstdc++6 (>= 4.1.1)")

set(CPACK_DEB_COMPONENT_INSTALL TRUE)

# Configure components
include(common/Deb.cmake)
include(master/Deb.cmake)
include(metalogger/Deb.cmake)
include(chunkserver/Deb.cmake)
include(cgiserv/Deb.cmake)

include(cgi/Deb.cmake)
include(client/Deb.cmake)

