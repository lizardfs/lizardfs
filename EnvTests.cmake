# comparing build config with (old) autotools build,
# refer to ced2e72776fd01e71f1c7eece9c4414b1fe23c54 / configure.ac

# discarded tests: 
# keywords tests for inline, volatile and const

# functions marked here as required, that don't appear in the code: atexit strchr

# checking for fork and vfork, but vfork unused

# XXX(lamvak): didn't add check for strerror_r function compatible with
# AC_FUNC_STRERROR_R

include(TestBigEndian)
include(CheckTypeSize)
include(CheckStructHasMember)
include(CheckFunctions)
include(CheckIncludes)
include(CheckMembers)

find_library(FUSE_LIBRARY fuse)
message(STATUS "FUSE_LIBRARY: ${FUSE_LIBRARY}")

find_library(RT_LIBRARY rt)
message(STATUS "RT_LIBRARY: ${RT_LIBRARY}")

find_package(ZLIB REQUIRED)
find_package(Socket)
find_package(Threads)
find_package(Boost COMPONENTS filesystem system regex thread)

set(INCLUDES arpa/inet.h fcntl.h inttypes.h limits.h netdb.h netinet/in.h stddef.h stdlib.h string.h
	sys/resource.h sys/rusage.h sys/socket.h sys/statvfs.h sys/time.h syslog.h unistd.h stdbool.h)
check_includes("${INCLUDES}")

TEST_BIG_ENDIAN(BIG_ENDIAN)
if(BIG_ENDIAN)
  set(WORDS_BIGENDIAN)
endif()

set(TYPES_CHECKED int8_t uint8_t int16_t uint16_t int32_t uint32_t int64_t
    uint64_t uid_t mode_t off_t pid_t size_t)
set(CMAKE_EXTRA_INCLUDE_FILES stdint.h)
foreach(TYPE ${TYPES_CHECKED})
  string(TOUPPER ${TYPE} TYPE_SIZE_VAR)
  CHECK_TYPE_SIZE(${TYPE} ${TYPE_SIZE_VAR})
endforeach()
set(CMAKE_EXTRA_INCLUDE_FILES)
#XXX(lamvak): now handle possible missing types or different type sizes

set(ST_MEMBERS_CHECKED st_blocks st_rdev st_birthtime st_blksize st_flags)
check_members("struct stat" "${ST_MEMBERS_CHECKED}" "sys/stat.h")
check_members("struct tm" "tm_gmtoff" "time.h")
check_members("struct rusage" "ru_maxrss" "sys/resource.h")

CHECK_FUNCTION_EXISTS(fork     HAVE_WORKING_FORK)
CHECK_FUNCTION_EXISTS(vfork    HAVE_WORKING_VFORK)

set(REQUIRED_FUNCTIONS atexit bzero ftruncate getaddrinfo getpass
  gettimeofday memmove memset mkdir realpath poll socket strchr strdup strtol
  strtoul ftello fseeko)
check_functions("${REQUIRED_FUNCTIONS}" TRUE)

set(OPTIONAL_FUNCTIONS strerror perror pread pwrite readv writev getrusage setitimer)
check_functions("${OPTIONAL_FUNCTIONS}" false)

set(CMAKE_REQUIRED_INCLUDES "sys/mman.h")
set(OPTIONAL_FUNCTIONS2 dup2 mlockall getcwd)
check_functions("${OPTIONAL_FUNCTIONS2}" false)
