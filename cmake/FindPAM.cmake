#  Copyright 2016-2017 Skytechnology sp. z o.o..
#
#  This file is part of LizardFS.
#
#  LizardFS is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, version 3.
#
#  LizardFS is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with LizardFS  If not, see <http://www.gnu.org/licenses/>.

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
