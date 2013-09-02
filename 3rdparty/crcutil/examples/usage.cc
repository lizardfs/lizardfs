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

#include "std_headers.h"
#include "interface.h"

static const size_t kRollWindow = 4;
static const unsigned char kTestData[] = "abcdefgh";

static const int kTestDataHead =
    static_cast<size_t>((sizeof(kTestData) - 1) / 4);
static const int kTestDataTail =
    static_cast<size_t>(sizeof(kTestData) - 1 - kTestDataHead);

typedef crcutil_interface::UINT64 uint64;

// GCC -- up to 4.5.0 inclusively -- is not aware that the right format
// to print "long long" is "%ll[oudx]". Such nonsense does not prevent
// it from complaining about format mismatch, though. Here is the cure.
void xprintf(const char *format, ...) {
  va_list va;
  va_start(va, format);
  vprintf(format, va);
  va_end(va);
  fflush(stdout);
}

//
// Please notice that when working with 64-bit and smaller CRCs,
// the use of "hi" part of CRC value is unnecessary.
//
void Show(const crcutil_interface::CRC *crc) {
  char buffer[sizeof(kTestData) + 32];

  //
  // Access CRC properties.
  //
  uint64 lo;
  crc->GeneratingPolynomial(&lo);
  xprintf("Generating polynomial 0x%llx, degree %llu",
          lo,
          static_cast<uint64>(crc->Degree()));
  crc->CanonizeValue(&lo);
  xprintf(", canonize_value=0x%llx", lo);

  crc->RollStartValue(&lo);
  xprintf(", roll start value=0x%llx, roll window=%llu",
          lo,
          static_cast<uint64>(crc->RollWindowBytes()));

  //
  // Check integrity of CRC tables.
  //
  crc->SelfCheckValue(&lo);
  xprintf(", self check value 0x%llx\n", lo);

  //
  // Compute CRC.
  //
  lo = 0;
  crc->Compute(kTestData, sizeof(kTestData) - 1, &lo);
  xprintf("CRC32C(\"%s\") = 0x%llx\n", kTestData, lo);

  //
  // Compute CRC (incrementally).
  //
  lo = 0;
  crc->Compute(kTestData, kTestDataHead, &lo);
  xprintf("CRC32C(\"%.*s\", 0) = 0x%llx, ", kTestDataHead, kTestData, lo);
  crc->Compute(kTestData + kTestDataHead, kTestDataTail, &lo);
  xprintf("CRC32C(\"%s\", CRC32(\"%.*s\", 0)) = 0x%llx = CRC32(\"%s\")\n",
      kTestData + kTestDataHead, kTestDataHead, kTestData, lo, kTestData);

  //
  // Compute CRC of a message filled with 0s.
  //
  lo = 1;
  crc->CrcOfZeroes(sizeof(buffer), &lo);

  uint64 lo1 = 1;
  memset(buffer, 0, sizeof(buffer));
  crc->Compute(buffer, sizeof(buffer), &lo1);
  xprintf("CRC of %d zeroes = %llx, expected %llx\n",
          static_cast<int>(sizeof(buffer)),
          lo,
          lo1);


  //
  // Use rolling CRC.
  //
  xprintf("RollingCrc expected =");
  for (size_t i = 0; i <= kRollWindow; ++i) {
    crc->RollStartValue(&lo);
    crc->Compute(kTestData + i, kRollWindow, &lo);
    xprintf(" 0x%llx", lo);
  }
  xprintf("\n");

  crc->RollStart(kTestData, &lo, NULL);
  xprintf("RollingCrc actual   = 0x%llx", lo);
  for (size_t i = 1; i <= kRollWindow; ++i) {
    crc->Roll(kTestData[i - 1], kTestData[i - 1 + kRollWindow], &lo, NULL);
    xprintf(" 0x%llx", lo);
  }
  xprintf("\n");

  //
  // Change initial value.
  //
  lo = 0;
  crc->Compute(kTestData, sizeof(kTestData) - 1, &lo);
  uint64 lo1_expected = 1;
  crc->Compute(kTestData, sizeof(kTestData) - 1, &lo1_expected);
  lo1 = lo;
  crc->ChangeStartValue(0, 0,   // old start value
                        1, 0,   // new start value
                        sizeof(kTestData) - 1,
                        &lo1);
  xprintf("CRC(\"%s\", 0) = 0x%llx, CRC(\"%s\", 1)=0x%llx, expected 0x%llx\n",
      kTestData, lo, kTestData, lo1, lo1_expected);

  //
  // Concatenate CRCs.
  //
  uint64 start_value = 1;
  lo = start_value;
  crc->Compute(kTestData, kTestDataHead, &lo);
  lo1 = 0;
  crc->Compute(kTestData + kTestDataHead, kTestDataTail, &lo1);

  uint64 lo2 = lo;
  crc->Concatenate(lo1, 0, kTestDataTail, &lo2);

  uint64 lo2_expected = start_value;
  crc->Compute(kTestData, sizeof(kTestData) - 1, &lo2_expected);

  xprintf("CRC(\"%.*s\", 1) = 0x%llx, CRC(\"%s\", 0)=0x%llx, "
         "CRC(\"%s\", 1) = 0x%llx, expected 0x%llx\n",
         kTestDataHead, kTestData, lo,
         kTestData + kTestDataHead, lo1,
         kTestData, lo2,
         lo2_expected);

  //
  // Store complementary CRC so that CRC of a message followed
  // by complementary CRC value produces predefined result (e.g. 0).
  //
  memcpy(buffer, kTestData, sizeof(kTestData) - 1);
  lo = 1;
  crc->Compute(buffer, sizeof(kTestData) - 1, &lo);
  size_t stored_crc_bytes = crc->StoreComplementaryCrc(
    buffer + sizeof(kTestData) - 1,
    lo, 0,
    0);

  // Compute CRC of message + complementary CRC using the same start value
  // (start value could be changed via ChangeStartValue()).
  lo1 = 1;
  crc->Compute(buffer, sizeof(kTestData) - 1 + stored_crc_bytes, &lo1);

  xprintf("Crc of message + complementary CRC = %llx, expected 0\n", lo1);

  //
  // Store CRC after the message and ensure that CRC of message + its
  // CRC produces constant result irrespective of message data.
  //
  memcpy(buffer, kTestData, sizeof(kTestData) - 1);
  lo = 1;
  crc->Compute(buffer, sizeof(kTestData) - 1, &lo);
  stored_crc_bytes = crc->StoreCrc(buffer + sizeof(kTestData) - 1, lo);

  // Compute CRC of message + its CRC using start value of 0.
  lo1 = 1;
  crc->Compute(buffer, sizeof(kTestData) - 1 + stored_crc_bytes, &lo1);

  // Ensure that it matches "predicted" constant value, irrespective
  // of a message or CRC start value.
  crc->CrcOfCrc(&lo2);
  xprintf("CrcOfCrc=%llx, expected %llx\n", lo1, lo2);

  xprintf("\n");
}

void ShowAndDelete(crcutil_interface::CRC *crc) {
  Show(crc);
  crc->Delete();
}

int main() {
  ShowAndDelete(crcutil_interface::CRC::Create(
      0xEB31D82E, 0, 32, true, 0x1111, 0, kRollWindow,
      crcutil_interface::CRC::IsSSE42Available(), NULL));
  ShowAndDelete(crcutil_interface::CRC::Create(
      0x82f63b78, 0, 32, true, 0x2222, 0, kRollWindow,
      crcutil_interface::CRC::IsSSE42Available(), NULL));
  return 0;
}
