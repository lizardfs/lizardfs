# - Try to find the Judy libraries
#
#  JUDY_FOUND - system has Judy
#  JUDY_INCLUDE_DIR - the Judy include directory

find_path(JUDY_INCLUDE_DIR NAMES Judy.h)
find_library(JUDY_LIBRARY Judy)

find_package_handle_standard_args(JUDY
                                  REQUIRED_VARS JUDY_LIBRARY JUDY_INCLUDE_DIR)
