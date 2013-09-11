/*
 2013-09-10 witekj@op.pl
 */

#ifndef  _CRC_SSE4__H__
#define  _CRC_SSE4__H__

#include "crcifc.h"
#include <inttypes.h>
#include <unistd.h>
//#include <MFSCommunication.h>
#include <vector>
using namespace std;

#define ZLIB_POLY 0xEDB88320U
#define INTEL_POLY 0xEDB88320U


typedef UINT64 CRC_UINT64;

struct CrcCalcWrap
{
    static CrcIfc* const m_CrcCalcArray[];
};

enum
{
    ZLIB_POLY_IDX = 0,
    INTEL_POLY_IDX
};


template< int T >
class LizardCrc
{
    public:
        static CRC_UINT64 mycrc32( CRC_UINT64 crc, const void* block, CRC_UINT64 leng )
        {
            CrcIfc* crcCalc = CrcCalcWrap::m_CrcCalcArray[ T ];
            CRC_UINT64 lo = 0;

            if( 0 == crc )  {
                crcCalc->Compute( block, static_cast< CRC_UINT64 >( leng ), &lo );
            }

            if( crc > 0 ) {
                CRC_UINT64 crcTmp = static_cast< CRC_UINT64 >( crc );
                crcCalc->Concatenate( lo, 0, static_cast< CRC_UINT64 >( leng ), &crcTmp );
                return static_cast< CRC_UINT64 >( crcTmp );
            }
            return static_cast< CRC_UINT64 >( lo );
        }

        static CRC_UINT64 mycrc32_combine( CRC_UINT64 crc1, CRC_UINT64 crc2, CRC_UINT64 leng2 )
        {
            CrcIfc* crcCalc = CrcCalcWrap::m_CrcCalcArray[ T ];
            CRC_UINT64 crcTmp1 = static_cast< CRC_UINT64 >( crc1 );
            crcCalc->Concatenate( static_cast< CRC_UINT64 >( crc2 ), 0,
                                  static_cast< CRC_UINT64 >( leng2 ), &crcTmp1 );
            return static_cast< CRC_UINT64 >( crcTmp1 );
        }
};


template< int T >
CRC_UINT64 mycrc32( CRC_UINT64 crc, const void* block, CRC_UINT64 leng )
{
    return LizardCrc< T >::mycrc32( crc, block, leng );
}


template< int T >
CRC_UINT64 mycrc32_combine( CRC_UINT64 crc1, CRC_UINT64 crc2, CRC_UINT64 leng2 )
{
    return LizardCrc< T >::mycrc32_combine( crc1, crc2, leng2 );
}


#endif
