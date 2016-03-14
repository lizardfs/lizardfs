if(APPLE)
  find_package(PkgConfig REQUIRED)
  if(PKG_CONFIG_FOUND)
    pkg_check_modules(PC_FUSE fuse)
  endif(PKG_CONFIG_FOUND)

  if(PC_FUSE_FOUND)
    set(FUSE_LIBRARY ${PC_FUSE_LIBRARIES})
    set(FUSE_LIBRARY_DIR ${PC_FUSE_LIBRARY_DIRS})
    set(FUSE_CFLAGS ${PC_FUSE_CFLAGS})
    set(FUSE_CFLAGS_OTHER  ${PC_FUSE_CFLAGS_OTHER})
    set(FUSE_VERSION_STRING ${PC_FUSE_VERSION})
    find_path(FUSE_INCLUDE_DIR "fuse/fuse.h" PATHS ${PC_FUSE_INCLUDE_DIRS} ${PC_FUSE_INCLUDE_DIRS}/..)
  endif()
else()
  find_library(FUSE_LIBRARY fuse)
  find_path(FUSE_INCLUDE_DIR "fuse/fuse.h")

  file(STRINGS "${FUSE_INCLUDE_DIR}/fuse/fuse_common.h" fuse_version_str REGEX "^#define[\t ]+FUSE.+VERSION[\t ]+[0-9]+")
  string(REGEX REPLACE ".*#define[\t ]+FUSE_MAJOR_VERSION[\t ]+([0-9]+).*" "\\1" fuse_version_major "${fuse_version_str}")
  string(REGEX REPLACE ".*#define[\t ]+FUSE_MINOR_VERSION[\t ]+([0-9]+).*" "\\1" fuse_version_minor "${fuse_version_str}")

  set(FUSE_VERSION_STRING "${fuse_version_major}.${fuse_version_minor}")
endif()

find_package_handle_standard_args(FUSE REQUIRED_VARS FUSE_LIBRARY FUSE_INCLUDE_DIR
                                     VERSION_VAR FUSE_VERSION_STRING)
