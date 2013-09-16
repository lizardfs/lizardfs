/*
 2013-09-10 witekj@op.pl
 */

#ifndef  _LIZARDFS_MFSCOMMON_LIZARD_CRC_H
#define  _LIZARDFS_MFSCOMMON_LIZARD_CRC_H

#include "crc_interface.h"
#include <inttypes.h>
#include <unistd.h>

#define PNG_POLYNOMIAL    0xEDB88320U
#define INTEL_POLYNOMIAL  0x82F63B78

struct CrcCalcCollection {
    static CrcInterface* const crcCalculator[];
};

enum CrcPolyIndexes {
    PNG_POLY_INDEX = 0,
    INTEL_POLY_INDEX
};

template< int T >
class LizardCrc
{
    public:
        static uint64_t crc32(uint64_t crc, const void* block, uint64_t leng)
        {
            CrcInterface* crcCalc = CrcCalcCollection::crcCalculator[ T ];
            uint64_t lo = 0;

            if ( 0 == crc )  {
	      crcCalc->Compute(block, static_cast< uint64_t >( leng ), &lo);
            }

            if ( crc > 0 )  {
              uint64_t crcTmp = static_cast< uint64_t >( crc );
              crcCalc->Concatenate(lo, 0, static_cast< uint64_t >( leng ), &crcTmp);
              return static_cast< uint64_t >( crcTmp );
            }
            return static_cast< uint64_t >( lo );
        }

        static uint64_t crc32Combine(uint64_t crc1, uint64_t crc2, uint64_t leng2)
        {
            CrcInterface* crcCalc = CrcCalcCollection::crcCalculator[ T ];
            uint64_t crcTmp1 = static_cast< uint64_t >(crc1);
            crcCalc->Concatenate( static_cast< uint64_t >(crc2), 0,
                                  static_cast< uint64_t >(leng2 ), &crcTmp1);
            return static_cast< uint64_t >(crcTmp1);
        }
};

template< int T >
uint64_t crc32(uint64_t crc, const void* block, uint64_t leng)
{
    return LizardCrc< T >::crc32(crc, block, leng);
}

template< int T >
uint64_t crc32_combine(uint64_t crc1, uint64_t crc2, uint64_t leng2)
{
    return LizardCrc< T >::crc32Combine(crc1, crc2, leng2);
}

#endif
