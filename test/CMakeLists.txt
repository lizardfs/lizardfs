#To build tests Google Test Framework is required.
#Availability:
# * Fedora 18 packages 'gtest' 'gtest-devel'
# * Debian 7 packages 'libgtest-dev'
#   Note: You need to compile after install from /usr/src/gtest and put *.a files to /usr/lib
# * sources for download from http://code.google.com/p/googletest/
find_package(GTest REQUIRED)

include_directories(../)
include_directories(${GTEST_INCLUDE_DIRS})

add_subdirectory(common)

