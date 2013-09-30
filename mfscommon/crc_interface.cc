#include "3rdparty/crcutil/std_headers.h"
#include "3rdparty/crcutil/aligned_alloc.h"
#include "3rdparty/crcutil/crc32c_sse4.h"
#include "3rdparty/crcutil/generic_crc.h"
#include "3rdparty/crcutil/protected_crc.h"
#include "3rdparty/crcutil/rolling_crc.h"
#include "crc_interface.h"

//using namespace std;

//using namespace crcutil;
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
    class Implementation : public CrcInterface {
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
    void *memory = crcutil::AlignedAlloc(sizeof(Self),
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
    crcutil::AlignedFree(this);
  }

  void *operator new(size_t, void *p) {
    return p;
  }

  virtual void Compute(const void *data,
                       size_t bytes,
                       /* INOUT */ uint64_t *lo,
                       /* INOUT */ uint64_t *hi = NULL) const {
    SetValue(crc_.CrcDefault(data, bytes, GetValue(lo, hi)), lo, hi);
  }

  virtual void Concatenate(uint64_t crcB_lo, uint64_t crcB_hi,
                           uint64_t bytes_B,
                           /* INOUT */ uint64_t* crcA_lo,
                           /* INOUT */ uint64_t* crcA_hi = NULL) const {
    SetValue(crc_.Base().Concatenate(GetValue(crcA_lo, crcA_hi),
                                     GetValue(crcB_lo, crcB_hi),
                                     bytes_B),
             crcA_lo,
             crcA_hi);
  }


 private:
  static Crc GetValue(uint64_t *lo, uint64_t *hi) {
    if (sizeof(Crc) <= sizeof(*lo)) {
      return crcutil::CrcFromUint64<Crc>(*lo);
    } else {
      return crcutil::CrcFromUint64<Crc>(*lo, *hi);
    }
  }

  static Crc GetValue(uint64_t lo, uint64_t hi) {
    return crcutil::CrcFromUint64<Crc>(lo, hi);
  }

  static void SetValue(const Crc& crc, uint64_t *lo, uint64_t *hi) {
    crcutil::Uint64FromCrc<Crc>(crc,
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

bool CrcInterface::IsSSE42Available() {
#if HAVE_AMD64 || HAVE_I386
  return crcutil::Crc32cSSE4::IsSSE42Available();
#else
  return false;
#endif  // HAVE_AMD64 || HAVE_I386
}

CrcInterface::~CrcInterface() {}
CrcInterface::CrcInterface() {}

CrcInterface *CrcInterface::Create(uint64_t poly_lo ) {

    uint64_t poly_hi = 0;
    const size_t degree = 32;
    bool canonical = true;
    uint64_t roll_start_value_lo = 0;
    uint64_t roll_start_value_hi = 0;
    size_t roll_length = 0;
    bool use_sse4_2 = CrcInterface::IsSSE42Available();
    const void **allocated_memory= NULL;

  #if CRCUTIL_USE_MM_CRC32 && (HAVE_I386 || HAVE_AMD64)
    if (use_sse4_2 &&
        degree == crcutil::Crc32cSSE4::FixedDegree() &&
        poly_lo == crcutil::Crc32cSSE4::FixedGeneratingPolynomial() &&
        poly_hi == 0) {
        if (roll_start_value_hi != 0 || (roll_start_value_lo >> 32) != 0) {
          return NULL;
        }
      return Implementation<crcutil::Crc32cSSE4, crcutil::RollingCrc32cSSE4>::Create(
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
    typedef crcutil::GenericCrc<crcutil::uint64, crcutil::uint64, crcutil::uint64, 4>
        Crc64;
    return Implementation<Crc64, crcutil::RollingCrc<Crc64> >::Create(
        poly_lo,
        degree,
        canonical,
        roll_start_value_lo,
        roll_length,
        allocated_memory);
  }
