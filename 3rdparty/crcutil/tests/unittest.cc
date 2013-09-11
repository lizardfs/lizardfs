// Copyright 2010 Google Inc.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "unittest.h"

extern "C" void SetHiPri();

using namespace crcutil;

#if !defined(HAVE_INT128)
#if defined(__GNUC__) && HAVE_AMD64
#define HAVE_INT128 1
#else
#define HAVE_INT128 0
#endif  // defined(__GNUC__) && HAVE_AMD64
#endif  // defined(HAVE_INT128)

#if HAVE_INT128
typedef unsigned int uint128_t __attribute__((mode(TI)));
#endif  // HAVE_INT128

int main(int argc, char **argv) {
  bool test_perf_main = true;
  bool test_perf_all = false;
  bool canonical = false;

  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "--noperf") == 0) {
      test_perf_main = false;
      test_perf_all = false;
    } else if (strcmp(argv[i], "--perfall") == 0) {
      test_perf_all = true;
    } else if (strcmp(argv[i], "--canonical") == 0) {
      canonical = true;
    } else if (strcmp(argv[i], "help") == 0) {
      fprintf(stderr, "Usage: unittest {options}\n");
      fprintf(stderr, "\n");
      fprintf(stderr, "Options:\n");
      fprintf(stderr, "    --canonical - test canonical variant of CRC\n");
      fprintf(stderr, "    --noperf    - do not test performance\n");
      fprintf(stderr, "    --perfall   - test performance of all CRC width "
              "(not just 32, 64, and 128)\n");
      fprintf(stderr, "\n");
      return 1;
    }
  }


  SetHiPri();

  CrcVerifier v;

  CreateTest<uint64, uint64, uint64>(
      64, 0, 0x9a6c9329ac4bc9b5ull, "u64/u64/u64", test_perf_main, &v);
  CreateTest<uint64, uint64, uint32>(
      64, 0, 0x9a6c9329ac4bc9b5ull, "u64/u64/u32", test_perf_all, &v);

  CreateTest<uint64, uint64, uint64>(
      32, 0, 0x82f63b78, "u64/u64/u64", test_perf_main, &v);
  CreateTest<uint32, uint32, uint32>(
      32, 0, 0x82f63b78, "u32/u32/u32", test_perf_main, &v);

  CreateTest<uint64, uint32, uint32>(
      32, 0, 0x82f63b78, "u64/u32/u32", test_perf_all, &v);
  CreateTest<uint64, uint32, uint64>(
      32, 0, 0x82f63b78, "u64/u32/u64", test_perf_all, &v);

  CreateTest<uint64, uint64, uint64>(
      15, 0, 0x00004CD1, "u64/u64/u64", test_perf_all, &v);
  CreateTest<uint32, uint32, uint32>(
      15, 0, 0x00004CD1, "u32/u32/u32", test_perf_all, &v);

  CreateTest<uint64, uint64, uint64>(
      07, 0, 0x00000048, "u64/u64/u64", test_perf_all, &v);
  CreateTest<uint32, uint32, uint32>(
      07, 0, 0x00000048, "u32/u32/u32", test_perf_all, &v);

#if HAVE_SSE2
  CreateTest<uint128_sse2, uint128_sse2, uint64>(
      128, 0xeca61dca77452c88ull, 0x21fe865c87bc0e61ull,
      "sse2/sse2/u64", test_perf_main, &v);
  CreateTest<uint128_sse2, uint128_sse2, uint32>(
      128, 0xeca61dca77452c88ull, 0x21fe865c87bc0e61ull,
      "sse2/sse2/u32", test_perf_main, &v);
  CreateTest<uint128_sse2, uint128_sse2, uint64>(
      64, 0, 0x9a6c9329ac4bc9b5ull,
      "sse2/sse2/u64", test_perf_main, &v);
  CreateTest<uint128_sse2, uint128_sse2, uint32>(
      64, 0, 0x9a6c9329ac4bc9b5ull,
      "sse2/sse2/u32", test_perf_main, &v);
  CreateTest<uint128_sse2, uint128_sse2, uint64>(
      32, 0, 0x82f63b78,
      "sse2/sse2/u64", test_perf_main, &v);
  CreateTest<uint128_sse2, uint128_sse2, uint32>(
      32, 0, 0x82f63b78,
      "sse2/sse2/u32", test_perf_main, &v);
#endif  // HAVE_SSE2
#if HAVE_INT128
  CreateTest<uint128_t, uint128_t, uint64>(
      128, 0xeca61dca77452c88ull, 0x21fe865c87bc0e61ull,
      "u128/u128/u64", test_perf_main, &v);
  CreateTest<uint128_t, uint128_t, uint32>(
      128, 0xeca61dca77452c88ull, 0x21fe865c87bc0e61ull,
      "u128/u128/u32", test_perf_main, &v);
#endif  // HAVE_INT128

  v.add(new CrcVerifierFactory<uint64, uint64, uint64, 2>(canonical,
      64, 0, 0x9a6c9329ac4bc9b5ull, "CRC-64-64/64/2", test_perf_main, true));
  v.add(new CrcVerifierFactory<uint64, uint64, uint64, 3>(canonical,
      64, 0, 0x9a6c9329ac4bc9b5ull, "CRC-64-64/64/3", test_perf_main, true));
  v.add(new CrcVerifierFactory<uint64, uint64, uint64, 4>(canonical,
      64, 0, 0x9a6c9329ac4bc9b5ull, "CRC-64-64/64/4", test_perf_main, true));
  v.add(new CrcVerifierFactory<uint64, uint64, uint64, 5>(canonical,
      64, 0, 0x9a6c9329ac4bc9b5ull, "CRC-64-64/64/5", test_perf_main, true));
  v.add(new CrcVerifierFactory<uint64, uint64, uint64, 6>(canonical,
      64, 0, 0x9a6c9329ac4bc9b5ull, "CRC-64-64/64/6", test_perf_main, true));
  v.add(new CrcVerifierFactory<uint64, uint64, uint64, 7>(canonical,
      64, 0, 0x9a6c9329ac4bc9b5ull, "CRC-64-64/64/7", test_perf_main, true));
  v.add(new CrcVerifierFactory<uint64, uint64, uint64, 8>(canonical,
      64, 0, 0x9a6c9329ac4bc9b5ull, "CRC-64-64/64/8", test_perf_main, true));

#if HAVE_SSE2
  v.add(new CrcVerifierFactory<uint128_sse2, uint128_sse2, size_t, 2>(
      canonical, 128, 0xeca61dca77452c88ull, 0x21fe865c87bc0e61ull,
      "CRC-128-sse2/size_t/2", test_perf_main, true));
  v.add(new CrcVerifierFactory<uint128_sse2, uint128_sse2, size_t, 3>(
      canonical, 128, 0xeca61dca77452c88ull, 0x21fe865c87bc0e61ull,
      "CRC-128-sse2/size_t/3", test_perf_main, true));
  v.add(new CrcVerifierFactory<uint128_sse2, uint128_sse2, size_t, 4>(
      canonical, 128, 0xeca61dca77452c88ull, 0x21fe865c87bc0e61ull,
      "CRC-128-sse2/size_t/4", test_perf_main, true));
  v.add(new CrcVerifierFactory<uint128_sse2, uint128_sse2, size_t, 5>(
      canonical, 128, 0xeca61dca77452c88ull, 0x21fe865c87bc0e61ull,
      "CRC-128-sse2/size_t/5", test_perf_main, true));
  v.add(new CrcVerifierFactory<uint128_sse2, uint128_sse2, size_t, 6>(
      canonical, 128, 0xeca61dca77452c88ull, 0x21fe865c87bc0e61ull,
      "CRC-128-sse2/size_t/6", test_perf_main, true));
  v.add(new CrcVerifierFactory<uint128_sse2, uint128_sse2, size_t, 7>(
      canonical, 128, 0xeca61dca77452c88ull, 0x21fe865c87bc0e61ull,
      "CRC-128-sse2/size_t/7", test_perf_main, true));
  v.add(new CrcVerifierFactory<uint128_sse2, uint128_sse2, size_t, 8>(
      canonical, 128, 0xeca61dca77452c88ull, 0x21fe865c87bc0e61ull,
      "CRC-128-sse2/size_t/8", test_perf_main, true));
#endif  // HAVE_SSE2

  v.add(new CrcVerifierFactory<size_t, size_t, size_t, 2>(canonical,
      32, 0, 0x82f63b78, "CRC-32-size_t/size_t/2", test_perf_main, true));
  v.add(new CrcVerifierFactory<size_t, size_t, size_t, 3>(canonical,
      32, 0, 0x82f63b78, "CRC-32-size_t/size_t/3", test_perf_main, true));
  v.add(new CrcVerifierFactory<size_t, size_t, size_t, 4>(canonical,
      32, 0, 0x82f63b78, "CRC-32-size_t/size_t/4", test_perf_main, true));
  v.add(new CrcVerifierFactory<size_t, size_t, size_t, 5>(canonical,
      32, 0, 0x82f63b78, "CRC-32-size_t/size_t/5", test_perf_main, true));
  v.add(new CrcVerifierFactory<size_t, size_t, size_t, 6>(canonical,
      32, 0, 0x82f63b78, "CRC-32-size_t/size_t/6", test_perf_main, true));
  v.add(new CrcVerifierFactory<size_t, size_t, size_t, 7>(canonical,
      32, 0, 0x82f63b78, "CRC-32-size_t/size_t/7", test_perf_main, true));
  v.add(new CrcVerifierFactory<size_t, size_t, size_t, 8>(canonical,
      32, 0, 0x82f63b78, "CRC-32-size_t/size_t/8", test_perf_main, true));

  v.TestFunctionality();
  v.TestPerformance();

  return (0);
}
