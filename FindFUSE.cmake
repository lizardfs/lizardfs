
find_package(PkgConfig REQUIRED)
if(PKG_CONFIG_FOUND)
    pkg_check_modules(PC_FUSE REQUIRED fuse)
    message(STATUS "FUSE version ${PC_FUSE_VERSION} found")
endif(PKG_CONFIG_FOUND)
