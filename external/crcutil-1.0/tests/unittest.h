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

// Functionality and performance tests of available CRC implementations.

#ifndef CRCUTIL_UNITTEST_H_
#define CRCUTIL_UNITTEST_H_

#if defined(_MSC_VER)
#define _CRT_SECURE_NO_WARNINGS
#pragma warning(push)
#pragma warning(disable: 4820)    // structure was padded
#pragma warning(disable: 4710)    // function not inlined
// C++ exception handler used, but unwind
// semantics are not enabled. Specify /EHsc
#pragma warning(disable: 4530)
#endif  // defined(_MSC_VER)

#include <map>
#include <set>
#include <string>

#if defined(_MSC_VER)
#pragma warning(pop)
#endif  // defined(_MSC_VER)

#include "aligned_alloc.h"
#include "bob_jenkins_rng.h"
#include "crc32c_sse4.h"
#include "generic_crc.h"
#include "rdtsc.h"
#include "rolling_crc.h"
#include "unittest_helper.h"

namespace crcutil {

#if defined(_INTEL_COMPILER)
#pragma warning(push)
#pragma warning(disable: 185)   // dynamic initialization in unreachable code
#endif  // defined(_INTEL_COMPILER)

#if HAVE_AMD64 || HAVE_I386

class Crc32cSSE4_Test : public Crc32cSSE4 {
 public:
  void *operator new(size_t, void *p) {
    return p;
  }
};

template<typename Crc, typename TableEntry, typename Word, int kStride>
class GenericCrcTest : public GenericCrc<Crc, TableEntry, Word, kStride> {
 public:
  ~GenericCrcTest() {
    AlignedFree(aligned_memory_);
  }

  void InitWithCrc32c(const Crc &generating_polynomial,
                      size_t degree,
                      bool constant) {
    this->Init(generating_polynomial, degree, constant);

    crc32c_ = NULL;
    aligned_memory_ = NULL;

    // Bug in GCC 4.4.3: if (kStride == 4) comparison is put first
    // so that the compiler may optimize out entire "if" statement,
    // GCC 4.4.3 at -O3 level generates incorrect code corrupting "this".
    if (degree == Crc32cSSE4::FixedDegree() &&
        generating_polynomial == Crc32cSSE4::FixedGeneratingPolynomial() &&
        (!CRCUTIL_USE_MM_CRC32 || Crc32cSSE4::IsSSE42Available()) &&
        kStride == 4 &&
        sizeof(Crc) == sizeof(size_t)) {
      aligned_memory_ = AlignedAlloc(sizeof(*crc32c_), 0, 256, NULL);
      crc32c_ = new(aligned_memory_) Crc32cSSE4_Test;
      crc32c_->Init(constant);
    }
  }

  bool HaveCrc32c() const {
    return (crc32c_ != NULL);
  }

  Crc CRC32C(const void *data, size_t bytes, const Crc &start) const {
    return static_cast<Crc>(
        crc32c_->CrcDefault(data, bytes, Downcast<Crc, size_t>(start)));
  }

 private:
  Crc32cSSE4_Test *crc32c_;
  void *aligned_memory_;
};

#else

template<typename Crc, typename TableEntry, typename Word, int kStride>
class GenericCrcTest : public GenericCrc<Crc, TableEntry, Word, kStride> {
 public:
  ~GenericCrcTest() {}
  void InitWithCrc32c(const Crc &generating_polynomial,
                      size_t degree,
                      bool constant) {
    Init(generating_polynomial, degree, constant);
  }

  bool HaveCrc32c() const {
    return false;
  }

  Crc CRC32C(const void * /* data */,
             size_t /* bytes */,
             const Crc & /* start */) const {
    return 0;
  }
};

#endif  // HAVE_AMD64 || HAVE_I386

// Forward declaration.
class PerfTestState;

class CrcVerifierInterface {
 public:
  virtual void TestFunctionality(const char *class_title) const = 0;
  virtual void TestPerformance(FILE *output,
                               PerfTestState *state,
                               bool multiword_only,
                               const char *class_title) = 0;
  CrcVerifierInterface() {}

 protected:
  virtual ~CrcVerifierInterface() {}
};

// Finds the most efficient implementation algorithm for
// given CRC.
class AlgSorter {
 public:
  void Add(std::string crc_name, std::string alg_name, double cycles) {
    CrcInfo *crc_info = crcs_[crc_name];
    if (crc_info == NULL) {
      crc_info = new CrcInfo;
      crcs_[crc_name] = crc_info;
    }
    AlgStat *alg_stat = (*crc_info)[alg_name];
    if (alg_stat == NULL) {
      alg_stat = new AlgStat;
      alg_stat->name = alg_name;
      alg_stat->cycles = 0;
      alg_stat->tests = 0;
      (*crc_info)[alg_name] = alg_stat;
    }
    alg_stat->cycles += cycles;
    alg_stat->tests += 1;
  }

  void PrintBest() {
    for (Crcs::const_iterator it = crcs_.begin(); it != crcs_.end(); ++it) {
      PrintBestCrc(it->first, *(it->second));
    }
  }

 private:
  struct AlgStat {
    std::string name;
    double cycles;
    int    tests;
  };

  typedef std::map<std::string, AlgStat *> CrcInfo;
  typedef std::map<std::string, CrcInfo *> Crcs;

  struct AlgStatCompare {
    bool operator()(const AlgStat *a, const AlgStat *b) const {
      return ((a->cycles / a->tests) < (b->cycles / b->tests));
    }
  };

  void PrintBestCrc(std::string crc_name, const CrcInfo &crc_info) {
    typedef std::set<AlgStat *, AlgStatCompare> SortedAlgStat;
    SortedAlgStat alg_set;
    for (CrcInfo::const_iterator it = crc_info.begin();
         it != crc_info.end();
         ++it) {
      alg_set.insert(it->second);
    }
    printf("%s\n", crc_name.c_str());
    for (SortedAlgStat::const_iterator it = alg_set.begin();
         it != alg_set.end();
         ++it) {
      printf("  %-28s %6.3f cycles/byte\n",
             (*it)->name.c_str(),
             (*it)->cycles / (*it)->tests);
    }
    printf("\n");
  }

  Crcs crcs_;
};


class PerfTestState {
 public:
  enum {
    MAX_TEST_COUNT = 100,
  };

  explicit PerfTestState(FILE *output)
      : output_(output), test_count_(0), title_printed_(false) {}
  ~PerfTestState() {}

  void Add(uint32 bytes, const char *name, double cycles) {
    if (test_count_ < MAX_TEST_COUNT) {
      bytes_ = bytes;
      test_[test_count_].name = name;
      test_[test_count_].cycles = cycles;
      test_count_ += 1;
    }

    if (bytes >= 1024 && bytes <= 1024 * 1024 &&
        memcmp(name, "MemSpeed", 9) != 0) {
      crc_sorter_.Add(crc_name_, std::string(name) + "-" + alg_name_, cycles);
    }
  }

  void Print() {
    if (!title_printed_) {
      for (size_t i = 0; i < test_count_; ++i) {
        fprintf(output_, ", %s", test_[i].name);
      }
      fprintf(output_, ",, BestTime, BestMethod");
      fprintf(output_, "\n");
      fflush(output_);
      title_printed_ = true;
    }
    double min_cycles = 1e30;
    size_t min_index = 0;
    fprintf(output_, "%9u", bytes_);
    for (size_t i = 0; i < test_count_; ++i) {
      fprintf(output_, ", %6.3f", test_[i].cycles);
      if (min_cycles > test_[i].cycles &&
          memcmp(test_[i].name, "MemSpeed", 9) != 0) {
        min_cycles = test_[i].cycles;
        min_index = i + 1;
      }
    }
    if (min_index != 0) {
      min_index -= 1;
      fprintf(output_,
              ",, %6.3f, %s",
              test_[min_index].cycles,
              test_[min_index].name);
    }
    fprintf(output_, "\n");
    fflush(output_);
    test_count_ = 0;
  }

  void Finish() {
    fprintf(output_, "\n");
    fflush(output_);
    test_count_ = 0;
    title_printed_ = false;
  }

  void StartNewCrc(const char *name) {
    // Assuming the name is in following format:
    // CRC-NNN-ALG-MODE where
    //     NNN - CRC width
    //     ALG - algorithm
    //     MODE - mode (raw or canonical)
    const char *second_dash;
    second_dash = strchr(name, '-');
    if (second_dash != NULL) {
      second_dash = strchr(second_dash + 1, '-');
    }
    if (second_dash == NULL) {
      second_dash = name + strlen(name);
    }
    std::string str_name(name, static_cast<size_t>(second_dash - name));
    crc_name_ = str_name;

    if (*second_dash == '-') {
      ++second_dash;
    }
    const char *last_dash = strrchr(second_dash, '-');
    if (last_dash == NULL) {
      last_dash = name + strlen(name);
    }
    std::string str_alg_name(second_dash,
                             static_cast<size_t>(last_dash - second_dash));
    alg_name_ = str_alg_name;
  }

  void PrintBest() {
    crc_sorter_.PrintBest();
  }

 private:
  PerfTestState();

  FILE *output_;
  size_t test_count_;
  bool title_printed_;
  uint32 bytes_;
  struct {
    const char *name;
    double cycles;
  } test_[MAX_TEST_COUNT];

  std::string crc_name_;
  std::string alg_name_;
  AlgSorter crc_sorter_;
};

template<typename CrcImplementation>
    class RollingCrcTest
    : public RollingCrc<CrcImplementation> {
 public:
  void *operator new(size_t, void *p) {
    return p;
  }
};

template<typename Crc, typename TableEntry, typename Word, int kStride>
    class CrcTest : public GenericCrcTest<Crc, TableEntry, Word, kStride>,
                    public CrcVerifierInterface {
 public:
  typedef CrcTest<Crc, TableEntry, Word, kStride> CrcTestSelf;
  typedef RollingCrcTest< GenericCrc<Crc, TableEntry, Word, kStride> >
      RollingCrcSelf;

  typedef Crc(CrcTestSelf::*CrcCallback) (
      const void *data, size_t bytes, const Crc &start) const;

  void *operator new(size_t, void *p) {
    return p;
  }

  void VerifyPow() const {
    Crc x = this->Base().One() >> 1;
    Crc x_pow_i = x;
    for (size_t i = 1; i < 1024; ++i) {
      Crc value = this->Base().XpowN(i);
      CHECK_EQ(x_pow_i, value);
      x_pow_i = this->Base().Multiply(x_pow_i, x);
    }
  }

  void VerifyCrcZeroes() const {
    uint8 buf[256];
    memset(buf, 0, sizeof(buf));
    for (size_t k = 0; k < 256; ++k) {
      Crc zero_old = this->CrcByte(buf, k, 1);
      Crc zero_new = this->Base().CrcOfZeroes(k, 1);
      CHECK_EQ(zero_old, zero_new);
    }
  }

  void VerifyChangeStartValue() const {
#if defined(_MSC_VER) && _MSC_FULL_VER <= 150030729 && defined(_M_IX86)
    // Work around a bug in CL.
    if (sizeof(Crc) > 8) {
      return;
    }
#endif  // defined(_MSC_VER) && _MSC_FULL_VER < 150030729 && defined(_M_IX86)
    uint8 buf[256];
    BobJenkinsRng<size_t> rng;
    for (size_t i = 0; i < 256; ++i) {
      buf[i] = static_cast<uint8>(rng.Get());
    }

    for (size_t n = 1; n < 256 && (n >> this->Base().Degree()) == 0; ++n) {
      for (size_t i = 0;
           i < 4096 && (i >> this->Base().Degree()) == 0;
           i += 11) {
        Crc crc_old = this->CrcByte(buf, n, 0);
        Crc crc_new = this->CrcByte(buf, n, static_cast<Crc>(i));
        Crc crc_chg = this->Base().ChangeStartValue(crc_old, n, 0,
                                                    static_cast<Crc>(i));
        CHECK_EQ(crc_new, crc_chg);
      }
    }
  }

  void VerifyConcatenate() const {
#if defined(_MSC_FULL_VER) && _MSC_FULL_VER <= 150030729 && defined(_M_IX86)
    // Work around a bug in CL
    if (sizeof(Crc) > 8) {
      return;
    }
#endif
    uint8 buf[256];
    BobJenkinsRng<size_t> rng;
    for (size_t i = 0; i < 256; ++i) {
      buf[i] = static_cast<uint8>(rng.Get());
    }
    for (size_t n = 1; n < 256; ++n) {
      for (size_t i = 0; i < n; ++i) {
        if (((i + 1) >> this->Base().Degree()) != 0) {
          break;
        }
        Crc startA = static_cast<Crc>(i + 1);
        Crc crc_A = this->CrcByte(buf, i, startA);
        Crc crc_B = this->CrcByte(buf + i, n - i, 0);
        Crc crc_AB = this->CrcByte(buf, n, startA);
        Crc crc = this->Base().Concatenate(crc_A, crc_B, n - i);
        CHECK_EQ(crc_AB, crc);
      }
    }
  }

  void VerifyCb(CrcCallback cb, const char *test_name) const {
    uint8 buf[256];

    fprintf(stderr, "  %s\n", test_name);
    fflush(stderr);

    // See whether it handles 0 bytes correctly.
    (this->*cb)(NULL, 0, 0);

    BobJenkinsRng<size_t> rng;

    for (size_t n = 0; n < 256; ++n) {
      memset(buf, 0, sizeof(buf));

      // Check "lonely 1" pattern.
      for (size_t k = 0; k < n; ++k) {
        buf[k] = 0x80;
        Crc crc_old = this->CrcByte(buf, n, 0);
        Crc crc_new = (this->*cb)(buf, n, 0);
        CHECK_EQ(crc_old, crc_new);
        buf[k] = 0;
      }

      // Check "lonely random byte" pattern.
      for (size_t k = 0; k < n; ++k) {
        buf[k] = static_cast<uint8>(rng.Get());
        Crc crc_old = this->CrcByte(buf, n, 0);
        Crc crc_new = (this->*cb)(buf, n, 0);
        CHECK_EQ(crc_old, crc_new);
        buf[k] = 0;
      }

      // Check "starts from sequence of 1s" pattern.
      for (size_t k = 0; k < n; ++k) {
        buf[k] = 0x80;
        Crc crc_old = this->CrcByte(buf, n, 0);
        Crc crc_new = (this->*cb)(buf, n, 0);
        CHECK_EQ(crc_old, crc_new);
      }

      // Check "starts from sequence of random bytes" pattern.
      memset(buf, 0, sizeof(buf));
      for (size_t k = 0; k < n; ++k) {
        buf[k] = static_cast<uint8>(rng.Get());
        Crc crc_old = this->CrcByte(buf, n, 0);
        Crc crc_new = (this->*cb)(buf, n, 0);
        CHECK_EQ(crc_old, crc_new);
      }
    }

    // Check that alignment is handled correctly.
    for (size_t n = 0; n < 256; ++n) {
      buf[n] = static_cast<uint8>(rng.Get());
      for (size_t k = 0; k < n; ++k) {
        Crc crc_old = this->CrcByte(buf + k, n - k, 0);
        Crc crc_new = (this->*cb)(buf + k, n - k, 0);
        CHECK_EQ(crc_old, crc_new);
      }
    }

    // Now, do randomized tests.
    size_t buffer_size = 128 * 1024;
    uint8 *buffer = new uint8[buffer_size];

    for (size_t i = 0; i < buffer_size; ++i) {
      buffer[i] = static_cast<uint8>(rng.Get());
    }

    for (size_t i = 0; i < 1 * 1000; ++i) {
      size_t bytes = rng.Get() % (buffer_size - 1);
      size_t offset = rng.Get() % (buffer_size - bytes);
      Crc crc_old = this->CrcByte(buffer + offset, bytes, 0);
      Crc crc_new = (this->*cb)(buffer + offset, bytes, 0);
      CHECK_EQ(crc_old, crc_new);
    }

    delete[] buffer;
  }

  void VerifyLCD() const {
    Crc A;
    Crc B;
    Crc LCD;
    for (A = this->Base().One(); A != 0; A >>= 1) {
      LCD = this->Base().FindLCD(A, &B);
      CHECK_EQ(this->Base().One(), LCD);
      Crc product = this->Base().Multiply(A, B);
      CHECK_EQ(this->Base().One(), product);
    }

    int rept = 253;
    uint64 step = static_cast<uint64>(1);
    size_t degree = this->Base().Degree();
    if (degree > 64) degree = 62;
    step = (step << degree) / rept;
    uint64 value = step;
    for (; --rept; value += step) {
      A = static_cast<Crc>(value);
      LCD = this->Base().FindLCD(A, &B);
      if (LCD == this->Base().One()) {
        Crc product = this->Base().Multiply(A, B);
        CHECK_EQ(this->Base().One(), product);
      }

      uint8 buf[64];
      size_t bytes = this->Base().StoreCrc(buf, A);
      Crc result = this->CrcByte(buf, bytes, A);
      CHECK_EQ(this->Base().CrcOfCrc(), result);

      bytes = this->Base().StoreComplementaryCrc(buf, A, 0);
      result = this->CrcByte(buf, bytes, A);
      CHECK_EQ(result, 0);
    }
  }

  void VerifyCrcOfCrc(void) const {
    Crc A;
    int rept = 253;
    uint64 step = static_cast<uint64>(1);
    size_t degree = this->Base().Degree();
    if (degree > 64) degree = 62;
    step = (step << degree) / rept;
    Crc expected = this->Base().CrcOfCrc();
    uint64 value = step;
    for (; --rept; value += step) {
      A = static_cast<Crc>(value);
      Crc value = this->CrcByte(&A, (this->Base().Degree() + 7) >> 3, A);
      CHECK_EQ(expected, value);
    }
  }

  void VerifyDistribution() const {
    Crc coef = this->Base().One();
    for (int index = 0; coef != 0; ++index, coef >>= 1) {
      int ones = 0;
      for (int i = 0; i < 256; ++i) {
        if ((this->crc_word_[sizeof(Word) - 1][i] & coef) == 0) {
          ones += 1;
        }
      }
      CHECK_EQ(128, ones);
    }
  }

  void VerifyRollingCrc() const {
    void *aligned_memory = AlignedAlloc(sizeof(RollingCrcSelf), 0, 256, NULL);
    RollingCrcSelf *rolling_crc = new(aligned_memory) RollingCrcSelf;

    static const size_t kMaxRollBytes = 8;
    static const size_t kBufferSize = 256 + kMaxRollBytes;
    uint8 buffer[kBufferSize];

    for (size_t starting_value = 0;
         starting_value < 256 &&
            (starting_value >> this->Base().Degree()) == 0;
         ++starting_value) {
      for (size_t roll_bytes = 1;
           roll_bytes < kMaxRollBytes;
           ++roll_bytes) {
        rolling_crc->Init(*this, roll_bytes, static_cast<Crc>(starting_value));

        memset(buffer, 0, sizeof(buffer));
        Crc old_crc = rolling_crc->Start(buffer);
        for (size_t offset = roll_bytes; offset < kBufferSize; ++offset) {
          Crc new_crc = rolling_crc->Roll(old_crc,
                                          buffer[offset - roll_bytes],
                                          buffer[offset]);
          Crc vfy_crc = this->CrcByte(buffer + offset - roll_bytes + 1,
                                      roll_bytes,
                                      static_cast<Crc>(starting_value));
          CHECK_EQ(new_crc, vfy_crc);
        }
        for (size_t offset = roll_bytes; offset < kBufferSize; ++offset) {
          buffer[offset] = static_cast<uint8>(offset);
          Crc new_crc = rolling_crc->Roll(old_crc,
                                          buffer[offset - roll_bytes],
                                          buffer[offset]);
          Crc vfy_crc = this->CrcByte(buffer + offset - roll_bytes + 1,
                                      roll_bytes,
                                      static_cast<Crc>(starting_value));
          CHECK_EQ(new_crc, vfy_crc);
          old_crc = new_crc;
        }
      }
    }

    AlignedFree(aligned_memory);
  }

  virtual void TestFunctionality(const char *class_title) const {
    fprintf(stderr,
           "Functional test of %s (size=%u bytes",
           class_title,
           static_cast<int>(
              sizeof(GenericCrc<Crc, TableEntry, Word, kStride>)));
    if (this->HaveCrc32c()) {
      fprintf(stderr, " [generic], %u bytes [CRC32C]",
          static_cast<int>(sizeof(Crc32cSSE4)));
    }
    fprintf(stderr, ")\n");
    fflush(stderr);

    VerifyPow();

    VerifyCb(&CrcTestSelf::CrcByteUnrolled, "ByteUnrolled");
    VerifyCb(&CrcTestSelf::CrcByteWord, "ByteWord");
    VerifyCb(&CrcTestSelf::CrcWord, "Word");
    VerifyCb(&CrcTestSelf::CrcBlockword, "Blockword");
    VerifyCb(&CrcTestSelf::CrcMultiword, "Multiword");
    if (this->HaveCrc32c()) {
      VerifyCb(&CrcTestSelf::CRC32C, "CRC32C");
    }

    VerifyLCD();
    VerifyCrcZeroes();
    VerifyChangeStartValue();
    VerifyConcatenate();
    VerifyCrcOfCrc();
    VerifyDistribution();

    VerifyRollingCrc();

    fprintf(stderr, "\n");
    fflush(stderr);
  }

  // MemSpeed sets the lowest threshold: it is necessary to touch
  // CRC table at least once per input byte -- and it is necessary
  // to touch all input bytes.
  Crc MemSpeed(const void *data, size_t bytes, const Crc &start) const {
    const uint8 *src = static_cast<const uint8 *>(data);
    const uint8 *end = src + bytes;
    Crc crc0 = start ^ this->Base().Canonize();
    size_t buf0 = 0;

    end -= 4 * sizeof(Word) - 1;
    if (src < end) {
      Crc crc1 = 0;
      Crc crc2 = 0;
      Crc crc3 = 0;
      do {
        for (size_t byte = 0; byte < sizeof(Word); ++byte) {
          crc0 ^= this->crc_word_interleaved_[0][0 * 64];
          crc1 ^= this->crc_word_interleaved_[1][1 * 64];
          crc2 ^= this->crc_word_interleaved_[2][2 * 64];
          crc3 ^= this->crc_word_interleaved_[3][3 * 64];
          buf0 ^= src[byte * sizeof(Word)];
        }
        src += 4 * sizeof(Word);
      } while (src < end);
      crc0 ^= crc1 ^ crc2 ^ crc3;
    }
    end += 4 * sizeof(Word) - 1;

    end -= sizeof(Word) - 1;
    while (src < end) {
      buf0 ^= *src;
      src += sizeof(Word);
      for (size_t byte = 0; byte < sizeof(Word); ++byte) {
        crc0 ^= this->crc_word_interleaved_[0][0];
      }
    }
    end += sizeof(Word) - 1;

    while (src < end) {
      buf0 ^= *src;
      src += 1;
      crc0 ^= this->crc_word_interleaved_[0][0];
    }

    return (crc0 ^ static_cast<Crc>(buf0));
  }

  void PerfTestCall(const uint8 *buf, size_t bytes, size_t rept,
      CrcCallback cb, const Crc &expected_crc) const {
    for (size_t r = 0; r < rept; ++r) {
      Crc crc = (this->*cb)(buf, bytes, 0);
      if (cb != &CrcTestSelf::MemSpeed) {
        CHECK_EQ(expected_crc, crc);
      }
    }
  }

  uint64 PerfTestMeasure(const uint8 *buf, size_t bytes, size_t rept,
      size_t tries, CrcCallback cb, const Crc &expected_crc) const {
    uint64 min_time = ~0ULL;
    for (size_t i = 0; i < tries; ++i) {
      uint64 t = Rdtsc::Get();
      PerfTestCall(buf, bytes, rept, cb, expected_crc);
      t = Rdtsc::Get() - t;
      if (min_time > t) {
        min_time = t;
      }
    }
    return min_time;
  }

  static const char *CompilerName() {
#define EXPANDED_STRINGIZE(c) #c
#define STRINGIZE(c) EXPANDED_STRINGIZE(c)
#if defined(__GNUC__)
    return "GCC-" STRINGIZE(__GNUC__) "." STRINGIZE(__GNUC_MINOR__) "."
           STRINGIZE(__GNUC_PATCHLEVEL__);
#elif defined(__INTEL_COMPILER)
    return "ICL-" STRINGIZE(__INTEL_COMPILER);
#elif defined(_MSC_FULL_VER)
    return "CL-" STRINGIZE(_MSC_FULL_VER);
#else
    return "Unknown";
#endif
#undef EXPANDED_STRINGIZE
#undef STRINGIZE
  }

  void PerfTest(PerfTestState *state,
                const uint8 *buf, uint32 bytes,
                CrcCallback cb, const char *title) const {
    Crc expected_crc = this->CrcByte(buf, bytes, 0);
    size_t tries = (64 * 1024 * 1024 + bytes - 1) / bytes;
    if (tries < 20) {
      tries = 20;
    } else if (tries > 1000) {
      tries = 1000;
    }
    size_t rept = (128 * 1024 * 1024 + tries*bytes - 1) / (tries*bytes);
    if (rept > 10000) {
      rept = 10000;
    }
    uint64 min_time = PerfTestMeasure(buf, bytes, rept, tries, cb,
                                      expected_crc);
    state->Add(bytes, title, min_time / (static_cast<double>(rept) * bytes));
    fflush(stderr);
  }

  uint64 PerfTestMeasureInit() {
    uint64 min_time = 0;
    min_time = ~min_time;
    for (size_t i = 0; i < 5; ++i) {
      uint64 t = Rdtsc::Get();
      this->InitWithCrc32c(this->Base().GeneratingPolynomial(),
                           this->Base().Degree(),
                           this->Base().Canonize() != 0);
      t = Rdtsc::Get() - t;
      if (min_time > t) {
        min_time = t;
      }
    }
    return min_time;
  }

  void PerfTestVariants(PerfTestState *state,
                        bool test_multiword_only,
                        const uint8 *buf, uint32 buf_bytes,
                        uint32 bytes) const {
    if (bytes > buf_bytes) {
      return;
    }
    if (!test_multiword_only) {
      PerfTest(state, buf, bytes,
               &CrcTestSelf::MemSpeed, "MemSpeed");
      PerfTest(state, buf, bytes,
               &CrcTestSelf::CrcByte, "Byte");
      PerfTest(state, buf, bytes,
               &CrcTestSelf::CrcByteUnrolled, "ByteUnrolled");
      PerfTest(state, buf, bytes,
               &CrcTestSelf::CrcByteWord, "ByteWord");
      PerfTest(state, buf, bytes,
               &CrcTestSelf::CrcWord, "Word");
    }
    PerfTest(state, buf, bytes,
             &CrcTestSelf::CrcBlockword, "Blockword");
    PerfTest(state, buf, bytes,
             &CrcTestSelf::CrcMultiword, "Multiword");
    if (this->HaveCrc32c()) {
      PerfTest(state, buf, bytes,
               &CrcTestSelf::CRC32C, "CRC32C");
    }
    state->Print();
  }

  void PerfTestRun(FILE *output,
                   PerfTestState *state,
                   bool multiword_only,
                   uint8 *buf,
                   int align,
                   uint32 buf_bytes) {
    BobJenkinsRng<size_t> rng;
    for (size_t i = 0; i < buf_bytes; ++i) {
      buf[i] = static_cast<uint8>(rng.Get());
    }

    fprintf(output, "ClassTitle, %s\n", class_title_);
    fprintf(output,
            "%12s, %7s, %6s, %3s, %11s, %4s, %6s, %9s, %10s, %10s\n",
            "Compiler",
            "CpuBits",
            "Degree",
            "Crc",
            "TableEntry",
            "Word",
            "Stride",
            "Alignment",
            "TableBytes",
            "InitCycles");
    fprintf(output,
            "%12s, %7d, %6d, %3d, %11d, %4d, %6d, %9d, %10d, %10d\n",
            CompilerName(),
            static_cast<int>(sizeof(size_t) * 8),
            static_cast<int>(this->Base().Degree()),
            static_cast<int>(sizeof(Crc)),
            static_cast<int>(sizeof(TableEntry)),
            static_cast<int>(sizeof(Word)),
            static_cast<int>(kStride),
            static_cast<int>(align),
            static_cast<int>(sizeof(CrcTestSelf)),
            static_cast<int>(PerfTestMeasureInit()));
    fprintf(output, "\n");
    fflush(output);

    state->StartNewCrc(class_title_);

    for (uint32 i = 4; i <= 64 * 1024 * 1024; i *= 2) {
      PerfTestVariants(state, multiword_only, buf, buf_bytes, i);
    }
    state->Finish();

#if defined(FULL_PERF_TEST)
    // Needed for performance work only.
    for (uint32 i = 1; i <= 128; ++i) {
      PerfTestVariants(state, multiword_only, buf, buf_bytes, i);
    }
    state->Finish();
#endif  // defined(FULL_PERF_TEST)
  }

  void TestPerformance(FILE *output,
                       PerfTestState *state,
                       bool multiword_only,
                       const char *class_title) {
    class_title_ = class_title;
    uint32 align = sizeof(Word);
    uint32 buf_bytes = 64*1024*1024;
    uint8 *buf0 = new uint8[buf_bytes + align*2];
    uint8 *buf = buf0;

    if ((reinterpret_cast<size_t>(buf) & (align - 1)) != 0) {
      buf += align - (reinterpret_cast<size_t>(buf) & (align - 1));
    }

    PerfTestRun(output, state, multiword_only, buf, 0, buf_bytes);

#if defined(FULL_PERF_TEST)
    // Needed for performance work only.
    PerfTestRun(output,
                state,
                multiword_only,
                buf + sizeof(Word)/2,
                sizeof(Word)/2,
                buf_bytes);
#endif  // defined(FULL_PERF_TEST)

    delete[] buf;
  }

 private:
  const char *class_title_;
};

class CrcVerifierFactoryInterface {
 public:
  virtual CrcVerifierInterface *Create(void *memory) const = 0;
  virtual size_t TellMemoryBytesNeeded() const = 0;
  virtual const char *class_title() const = 0;
  virtual bool test_performance() const = 0;
  virtual bool multiword_only() const = 0;
  CrcVerifierFactoryInterface() {}
  virtual ~CrcVerifierFactoryInterface() {}
};

// Verifies functionality of all registered CRCs first
// and only then runs time-consuming performance tests.
// That is helpful during development: we do not want to wait for
// all performance tests to go through but still want to do full
// functionality test.
class CrcVerifier {
 public:
  CrcVerifier() : memory_needed_(0), factory_count_(0) {}

  void add(const CrcVerifierFactoryInterface *factory) {
    if (factory_count_ < MAX_FACTORY_COUNT) {
      factory_[factory_count_] = factory;
      factory_count_ += 1;
      size_t memory_needed = factory->TellMemoryBytesNeeded();
      if (memory_needed_ < memory_needed) {
        memory_needed_ = memory_needed;
      }
    }
  }

  void TestFunctionality() {
    fprintf(stderr, "Verifying functionality\n");
    fflush(stderr);

    memory_allocate();
    for (size_t i = 0; i < factory_count_; ++i) {
      const CrcVerifierFactoryInterface *factory = factory_[i];
      const CrcVerifierInterface *instance = factory->Create(aligned_memory_);
      instance->TestFunctionality(factory->class_title());
    }
    memory_release();
  }

  void TestPerformance() {
    fprintf(stderr, "Verifying performance\n");
    fflush(stderr);

    FILE *output = stdout;
    PerfTestState state(output);

    memory_allocate();
    for (size_t i = 0; i < factory_count_; ++i) {
      const CrcVerifierFactoryInterface *factory = factory_[i];
      if (!factory->test_performance()) {
        continue;
      }
      CrcVerifierInterface *instance = factory->Create(aligned_memory_);
      instance->TestPerformance(output,
                                &state,
                                factory->multiword_only(),
                                factory->class_title());
    }

    state.PrintBest();

    memory_release();
  }

  ~CrcVerifier() {
    for (size_t i = 0; i < factory_count_; ++i) {
      delete factory_[i];
    }
  }


 private:
  void memory_allocate() {
    aligned_memory_ = AlignedAlloc(memory_needed_, 0, 256, NULL);
  }

  void memory_release() {
    AlignedFree(aligned_memory_);
  }

  enum {
    MAX_FACTORY_COUNT = 100,
  };
  void *aligned_memory_;
  size_t memory_needed_;
  size_t factory_count_;
  const CrcVerifierFactoryInterface *factory_[MAX_FACTORY_COUNT];
};

// Provides a factory creating ins
//
template<typename Crc, typename TableEntry, typename Word, size_t kStride>
    class CrcVerifierFactory : public CrcVerifierFactoryInterface {
 public:
  typedef CrcTest<Crc, TableEntry, Word, kStride> CrcTestSelf;
  typedef CrcVerifierFactory<Crc, TableEntry, Word, kStride>
          CrcVerifierFactorySelf;

  CrcVerifierFactory(bool constant, size_t degree,
                     uint64 poly_hi, uint64 poly_lo,
                     const char *class_title,
                     bool test_performance,
                     bool multiword_only)
      : degree_(degree),
        constant_(constant),
        test_performance_(test_performance),
        multiword_only_(multiword_only) {
    strcpy(class_title_, class_title);

    // On 32-bit platforms, "this" may be misaligned with respect
    // to sizeof(Crc) -- even though alignment of the structure is
    // correct (e.g. 16 when Crc=uint128_sse2), "new" returns memory
    // that is aligned on 8 boundary only. Respectively, attempt
    // to access a field of Crc type when Crc=uint128_sse2 fails.
    //
    // Use local variable and memcpy to access fields of Crc type.
    //
    Crc generating_polynomial = CrcFromUint64<Crc>(poly_lo, poly_hi);
    memcpy(&generating_polynomial_, &generating_polynomial, sizeof(Crc));
  }

  virtual CrcVerifierInterface *Create(void *memory) const {
    CrcTestSelf *crc = new(memory) CrcTestSelf;
    // "this" may be misaligned on 32-bit platforms.
    // Use local variable and memcpy to access fields of Crc type.
    Crc generating_polynomial;
    memcpy(&generating_polynomial, &generating_polynomial_, sizeof(Crc));
    crc->InitWithCrc32c(generating_polynomial, degree_, constant_);
    return crc;
  }

  virtual size_t TellMemoryBytesNeeded() const {
    return sizeof(CrcTestSelf);
  }

  const char *class_title() const { return class_title_; }
  bool test_performance() const { return test_performance_; }
  bool multiword_only() const { return multiword_only_; }

 private:
  CrcVerifierFactory() {}
  const CrcVerifierFactory &operator=(const CrcVerifierFactory &src);

  Crc generating_polynomial_;
  char class_title_[128];
  size_t const degree_;
  bool const constant_;
  bool const test_performance_;
  bool const multiword_only_;
};

// Adds 2 tests to the verifier: one full test with 4 stripes,
// and one reduced test (only Blockword and Multiword) for 3 stripes.
template<typename Crc, typename TableEntry, typename Word>
    void CreateTest(size_t degree,
                    uint64 poly_hi,
                    uint64 poly_lo,
                    const char *title,
                    bool test_performance,
                    CrcVerifier *v) {
  char name[128];
  bool canonical = true;
  for (;;) {
    sprintf(name, "CRC-%d-%s-4-%s", static_cast<int>(degree),
            title, (canonical ? "canonical" : "raw"));
    v->add(new CrcVerifierFactory<Crc, TableEntry, Word, 4>(canonical,
                                                            degree,
                                                            poly_hi,
                                                            poly_lo,
                                                            name,
                                                            test_performance,
                                                            false));

    if (degree == 128 && HAVE_AMD64) {
      sprintf(name, "CRC-%d-%s-6-%s", static_cast<int>(degree),
              title, (canonical ? "canonical" : "raw"));
      v->add(new CrcVerifierFactory<Crc, TableEntry, Word, 6>(canonical,
                                                              degree,
                                                              poly_hi,
                                                              poly_lo,
                                                              name,
                                                              test_performance,
                                                              true));
    }

    sprintf(name, "CRC-%d-%s-3-%s", static_cast<int>(degree),
            title, (canonical ? "canonical" : "raw"));
    v->add(new CrcVerifierFactory<Crc, TableEntry, Word, 3>(canonical,
                                                            degree,
                                                            poly_hi,
                                                            poly_lo,
                                                            name,
                                                            test_performance,
                                                            true));
    if (!canonical) {
      break;
    }
    canonical = false;
    test_performance = false;
  }
}

#if defined(_INTEL_COMPILER)
#pragma warning(pop)
#endif  // defined(_INTEL_COMPILER)

}  // namespace crcutil

#endif  // CRCUTIL_UNITTEST_H_
