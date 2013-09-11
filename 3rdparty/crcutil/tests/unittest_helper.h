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

// A set of useful macros for crcutil_unittest.

#ifndef CRCUTIL_UNITTEST_HELPER_H_
#define CRCUTIL_UNITTEST_HELPER_H_

#include "std_headers.h"    // printf

#if !defined(CHECK)

#if defined(_MSC_VER)
#define DEBUG_BREAK() __debugbreak()
#else
#define DEBUG_BREAK() exit(1)
#endif  // defined(_MSC_VER)

#define CHECK(cond) do { \
  if (!(cond)) { \
    fprintf(stderr, "%s, %d: ASSERT(%s)\n", __FILE__, __LINE__, #cond); \
    fflush(stderr); \
    DEBUG_BREAK(); \
  } \
} while (0)


#define CHECK_GE(a, b) CHECK((a) >= (b))
#define CHECK_NE(a, b) CHECK((a) != (b))
#define CHECK_EQ(a, b) CHECK((a) == (b))

#endif  // !defined(CHECK)

#endif  // CRCUTIL_UNITTEST_HELPER_H_
