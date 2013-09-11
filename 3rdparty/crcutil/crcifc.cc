#include "crcifc.h"
#include "aligned_alloc.h"
#include "crc32c_sse4.h"
#include "generic_crc.h"
#include "protected_crc.h"
#include "rolling_crc.h"

using namespace crcutil;
// Align all CRC tables on kAlign boundary.
// Shall be exact power of 2.
static size_t kAlign = 4 * 1024;

#if defined(__GNUC__)
// Suppress 'invalid access to non-static data member ...  of NULL object'
#undef offsetof
#define offsetof(TYPE, MEMBER) (reinterpret_cast <size_t> \
    ((&reinterpret_cast <const char &>( \
        reinterpret_cast <const TYPE *>(1)->MEMBER))) - 1)
#endif  // defined(__GNUC__)



template<typename CrcImplementation, typename RollingCrcImplementation>
    class Implementation : public CrcIfc {
 public:
  typedef typename CrcImplementation::Crc Crc;
  typedef Implementation<CrcImplementation, RollingCrcImplementation> Self;

  Implementation(const Crc& poly,
                 size_t degree,
                 bool canonical,
                 const Crc &roll_start_value,
                 size_t roll_length)
    : crc_(poly, degree, canonical),
      rolling_crc_(crc_, roll_length, roll_start_value) {
  }

  static Self *Create(const Crc& poly,
                      size_t degree,
                      bool canonical,
                      const Crc &roll_start_value,
                      size_t roll_length,
                      const void **allocated_memory) {
    void *memory = AlignedAlloc(sizeof(Self),
                                offsetof(Self, crc_),
                                kAlign,
                                allocated_memory);
    return new(memory) Self(poly,
                            degree,
                            canonical,
                            roll_start_value,
                            roll_length);
  }

  virtual void Delete() {
    AlignedFree(this);
  }

  void *operator new(size_t, void *p) {
    return p;
  }

  virtual void Compute(const void *data,
                       size_t bytes,
                       /* INOUT */ UINT64 *lo,
                       /* INOUT */ UINT64 *hi = NULL) const {
    SetValue(crc_.CrcDefault(data, bytes, GetValue(lo, hi)), lo, hi);
  }

  virtual void Concatenate(UINT64 crcB_lo, UINT64 crcB_hi,
                           UINT64 bytes_B,
                           /* INOUT */ UINT64* crcA_lo,
                           /* INOUT */ UINT64* crcA_hi = NULL) const {
    SetValue(crc_.Base().Concatenate(GetValue(crcA_lo, crcA_hi),
                                     GetValue(crcB_lo, crcB_hi),
                                     bytes_B),
             crcA_lo,
             crcA_hi);
  }


 private:
  static Crc GetValue(UINT64 *lo, UINT64 *hi) {
    if (sizeof(Crc) <= sizeof(*lo)) {
      return CrcFromUint64<Crc>(*lo);
    } else {
      return CrcFromUint64<Crc>(*lo, *hi);
    }
  }

  static Crc GetValue(UINT64 lo, UINT64 hi) {
    return CrcFromUint64<Crc>(lo, hi);
  }

  static void SetValue(const Crc& crc, UINT64 *lo, UINT64 *hi) {
    Uint64FromCrc<Crc>(crc,
                       reinterpret_cast<crcutil::uint64 *>(lo),
                       reinterpret_cast<crcutil::uint64 *>(hi));
  }

  const CrcImplementation crc_;
  const RollingCrcImplementation rolling_crc_;

  const Self &operator =(const Self &) {}
};

#if defined(_MSC_VER)
// 'use_sse4_2' : unreferenced formal parameter
#pragma warning(disable: 4100)
#endif  // defined(_MSC_VER)

bool CrcIfc::IsSSE42Available() {
#if HAVE_AMD64 || HAVE_I386
  return Crc32cSSE4::IsSSE42Available();
#else
  return false;
#endif  // HAVE_AMD64 || HAVE_I386
}

CrcIfc::~CrcIfc() {}
CrcIfc::CrcIfc() {}

CrcIfc *CrcIfc::Create(UINT64 poly_lo ) {

    UINT64 poly_hi = 0;
    const size_t degree = 32;
    bool canonical = true;
    UINT64 roll_start_value_lo = 0;
    UINT64 roll_start_value_hi = 0;
    size_t roll_length = 0;
    bool use_sse4_2 = CrcIfc::IsSSE42Available();
    const void **allocated_memory= NULL;

  #if CRCUTIL_USE_MM_CRC32 && (HAVE_I386 || HAVE_AMD64)
    if (use_sse4_2 &&
        degree == Crc32cSSE4::FixedDegree() &&
        poly_lo == Crc32cSSE4::FixedGeneratingPolynomial() &&
        poly_hi == 0) {
        if (roll_start_value_hi != 0 || (roll_start_value_lo >> 32) != 0) {
          return NULL;
        }
      return Implementation<Crc32cSSE4, RollingCrc32cSSE4>::Create(
          static_cast<size_t>(poly_lo),
          degree,
          canonical,
          static_cast<size_t>(roll_start_value_lo),
          static_cast<size_t>(roll_length),
          allocated_memory);
    }
  #endif  // CRCUTIL_USE_MM_CRC32 && (HAVE_I386 || HAVE_AMD64)

    if (poly_hi != 0 || (degree != 64 && (poly_lo >> degree) != 0)) {
      return NULL;
    }
    if (roll_start_value_hi != 0 ||
        (degree != 64 && (roll_start_value_lo >> degree) != 0)) {
      return NULL;
    }
    typedef GenericCrc<crcutil::uint64, crcutil::uint64, crcutil::uint64, 4>
        Crc64;
    return Implementation<Crc64, RollingCrc<Crc64> >::Create(
        poly_lo,
        degree,
        canonical,
        roll_start_value_lo,
        roll_length,
        allocated_memory);
  }
