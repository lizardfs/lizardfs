AM_CXXFLAGS=-DCRCUTIL_USE_MM_CRC32=1 -Wall -msse2 -Icode
AM_CFLAGS=$(AM_CXXFLAGS)
check_PROGRAMS=crcutil_ut
TESTS=crcutil_ut
crcutil_ut_SOURCES=code/base_types.h code/crc32c_sse4.cc code/crc32c_sse4.h code/crc32c_sse4_intrin.h code/crc_casts.h code/generic_crc.h code/gf_util.h code/multiword_128_64_gcc_amd64_sse2.cc code/multiword_64_64_cl_i386_mmx.cc code/multiword_64_64_gcc_amd64_asm.cc code/multiword_64_64_gcc_i386_mmx.cc code/platform.h code/protected_crc.h code/rolling_crc.h code/std_headers.h code/uint128_sse2.h tests/aligned_alloc.h tests/bob_jenkins_rng.h tests/rdtsc.h tests/set_hi_pri.c tests/unittest.cc tests/unittest.h tests/unittest_helper.h 
tmpdir=/tmp
tmp_PROGRAMS=usage
usage_CXXFLAGS=$(AM_CXXFLAGS) -Itests
usage_SOURCES=code/base_types.h code/crc32c_sse4.cc code/crc32c_sse4.h code/crc32c_sse4_intrin.h code/crc_casts.h code/generic_crc.h code/gf_util.h code/multiword_128_64_gcc_amd64_sse2.cc code/multiword_64_64_cl_i386_mmx.cc code/multiword_64_64_gcc_amd64_asm.cc code/multiword_64_64_gcc_i386_mmx.cc code/platform.h code/protected_crc.h code/rolling_crc.h code/std_headers.h code/uint128_sse2.h examples/interface.cc examples/interface.h examples/usage.cc tests/aligned_alloc.h 
