# - Try to find the PAM libraries
#
#  PAM_FOUND - system has pam
#  PAM_INCLUDE_DIR - the pam include directory
#  PAM_LIBRARIES - pam libraries

find_path(PAM_INCLUDE_DIR NAMES pam_appl.h PATH_SUFFIXES security pam)
find_library(PAM_LIBRARY pam)
find_library(PAM_MISC_LIBRARY pam_misc)

if(PAM_LIBRARY AND PAM_MISC_LIBRARY)
  set(PAM_LIBRARIES ${PAM_LIBRARY} ${PAM_MISC_LIBRARY})
endif()

find_package_handle_standard_args(PAM
                                  REQUIRED_VARS PAM_LIBRARY PAM_MISC_LIBRARY PAM_INCLUDE_DIR)
