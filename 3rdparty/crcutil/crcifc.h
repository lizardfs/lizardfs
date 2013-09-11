#ifndef _LIZARD_CRC_INTERFACE_H_
#define _LIZARD_CRC_INTERFACE_H_

#include "std_headers.h"
#include <stdint.h>

typedef uint64_t UINT64;

class CrcIfc {
 public:
    static CrcIfc *Create( UINT64 poly );
    virtual void Delete() = 0;
    static bool IsSSE42Available();
    virtual void Compute(const void *data, size_t bytes, UINT64 *lo, UINT64 *hi = NULL) const = 0;
    virtual void Concatenate(UINT64 crcB_lo, UINT64 crcB_hi,
                           UINT64 bytes_B,
                           /* INOUT */ UINT64* crcA_lo,
                           /* INOUT */ UINT64* crcA_hi = NULL) const = 0;

  protected:

  virtual ~CrcIfc();
  CrcIfc();
};


#endif
