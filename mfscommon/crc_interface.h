#ifndef _LIZARDFS_MFSCOMMON_CRC_INTERFACE_H_
#define _LIZARDFS_MFSCOMMON_CRC_INTERFACE_H_

#include <stddef.h>
#include <stdint.h>

class CrcInterface {
 public:
    static CrcInterface *Create(uint64_t poly);
    virtual void Delete() = 0;
    static bool IsSSE42Available();
    virtual void Compute(const void *data, size_t bytes, uint64_t *lo, uint64_t *hi = NULL) const = 0;
    virtual void Concatenate(uint64_t crcB_lo,
                             uint64_t crcB_hi,
                             uint64_t bytes_B,
                             uint64_t* crcA_lo,
                             uint64_t* crcA_hi = NULL) const = 0;

  protected:

  virtual ~CrcInterface();
  CrcInterface();
};

#endif
