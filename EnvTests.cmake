# comparing build config with (old) autotools build,
# refer to ced2e72776fd01e71f1c7eece9c4414b1fe23c54 / configure.ac

# discarded tests:
# keywords tests for inline, volatile and const

# functions marked here as required, that don't appear in the code: atexit strchr

# checking for fork and vfork, but vfork unused

# XXX(lamvak): didn't add check for strerror_r function compatible with
# AC_FUNC_STRERROR_R

include(CheckCXXCompilerFlag)
include(CheckCXXExpression.cmake)
include(CheckCXXSourceCompiles)
include(CheckFunctionExists)
include(CheckFunctions)
include(CheckIncludes)
include(CheckLibraryExists)
include(CheckMembers)
include(CheckStructHasMember)
include(CheckSymbolExists)
include(CheckTypeSize)
include(TestBigEndian)

set(INCLUDES arpa/inet.h fcntl.h inttypes.h limits.h netdb.h
    netinet/in.h stddef.h stdlib.h string.h sys/mman.h
    sys/resource.h sys/rusage.h sys/socket.h sys/statvfs.h sys/time.h
    syslog.h unistd.h stdbool.h isa-l/erasure_code.h
)


TEST_BIG_ENDIAN(BIG_ENDIAN)
if(BIG_ENDIAN)
  set(WORDS_BIGENDIAN 1)
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

CHECK_FUNCTION_EXISTS(fork     LIZARDFS_HAVE_WORKING_FORK)
CHECK_FUNCTION_EXISTS(vfork    LIZARDFS_HAVE_WORKING_VFORK)
CHECK_TEMPLATE_FUNCTION_EXISTS("string" "std::to_string(0)" LIZARDFS_HAVE_STD_TO_STRING)
CHECK_TEMPLATE_FUNCTION_EXISTS("string" "std::stoull(\"0\")" LIZARDFS_HAVE_STD_STOULL)

set(REQUIRED_FUNCTIONS atexit ftruncate gettimeofday memmove memset mkdir strchr strdup strtol
    strtoul ftello fseeko)
if(NOT MINGW)
  list(APPEND REQUIRED_FUNCTIONS getpass poll realpath)
endif()
check_functions("${REQUIRED_FUNCTIONS}" TRUE)

set(OPTIONAL_FUNCTIONS strerror perror pread pwrite readv writev getrusage
  setitimer posix_fadvise fallocate)
check_functions("${OPTIONAL_FUNCTIONS}" false)

CHECK_LIBRARY_EXISTS(rt clock_gettime "time.h" LIZARDFS_HAVE_CLOCK_GETTIME)

set(CMAKE_REQUIRED_INCLUDES "sys/mman.h")
set(OPTIONAL_FUNCTIONS2 dup2 mlockall getcwd)
check_functions("${OPTIONAL_FUNCTIONS2}" false)

set(CMAKE_REQUIRED_FLAGS "-std=c++11")
check_cxx_expression(::std::chrono::steady_clock::is_steady chrono LIZARDFS_HAVE_STD_CHRONO_STEADY_CLOCK)
check_cxx_expression("sizeof(::std::allocator_traits<std::allocator<int*>>::pointer)" memory LIZARDFS_HAVE_STD_ALLOCATOR_TRAITS)
unset(CMAKE_REQUIRED_FLAGS)

check_cxx_compiler_flag(-mcrc32    CXX_HAS_MCRC32)

set(_CHECK_CXX_CPU_CHECK_CODE "
__attribute__ ((target (\"default\"))) int test_default() { return 0; }
__attribute__ ((target (\"sse\"))) int test_sse() { return 1; }
__attribute__ ((target (\"ssse3\"))) int test_ssse3() { return 2; }
int main() { if (__builtin_cpu_supports(\"ssse3\")) return test_ssse3(); else return test_default(); }
")
check_cxx_source_compiles("${_CHECK_CXX_CPU_CHECK_CODE}" LIZARDFS_HAVE_CPU_CHECK)

if(APPLE)
    set(SOCKET_CONVERT_POLL_TO_SELECT 1)
endif()

set(CMAKE_REQUIRED_FLAGS "-D_GNU_SOURCE")
check_symbol_exists(FALLOC_FL_PUNCH_HOLE "fcntl.h" LIZARDFS_HAVE_FALLOC_FL_PUNCH_HOLE)
unset(CMAKE_REQUIRED_FLAGS)
