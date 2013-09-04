/* 2013/09/31 witekj@op.pl
 *
*/

#ifndef  _CRC_SSE4__H__
#define  _CRC_SSE4__H__

#include <inttypes.h>
#include <unistd.h>
#include <utility>
#include <interface.h>
#include <map>
#include <auto_ptr.h>
#include <MFSCommunication.h>
using namespace std;

typedef crcutil_interface::UINT64 CRC_UINT64;

const CRC_UINT64 INTEL_CRC = 0x82F63B78;


class CrcContainer
{
public:
	typedef map< pair< __pid_t, CRC_UINT64 >, crcutil_interface::CRC* > CRC_BAG_TYPE;
    static crcutil_interface::CRC* getCrc( const CRC_UINT64& pPoly );

private:
	 static CRC_BAG_TYPE m_CrcBag;
	 static crcutil_interface::CRC* createInstance( const CRC_UINT64& pPoly );
};


template< CRC_UINT64 T >
class LizardCrc
{
    public:
		static uint32_t mycrc32( uint32_t crc, const uint8_t* block,uint32_t leng )
		{
			crcutil_interface::CRC* crcCalc = CrcContainer::getCrc( T );
		    CRC_UINT64 lo = 0;

			if( 0 == crc )  {
				crcCalc->Compute( block, static_cast< CRC_UINT64 >( leng ), &lo );
			}

			if( crc > 0 ) {
				CRC_UINT64 crcTmp = static_cast< CRC_UINT64 >( crc );
				crcCalc->Concatenate( lo, 0, static_cast< CRC_UINT64 >( leng ), &crcTmp );
				return static_cast< uint32_t >( crcTmp );
			}
			return static_cast< uint32_t >( lo );
		}

        static uint32_t mycrc32_combine( uint32_t crc1, uint32_t crc2, uint32_t leng2 ) 
		{
			crcutil_interface::CRC* crcCalc = CrcContainer::getCrc( T );
			CRC_UINT64 crcTmp1 = static_cast< CRC_UINT64 >( crc1 );
			crcCalc->Concatenate( static_cast< CRC_UINT64 >( crc2 ), 0, static_cast< CRC_UINT64 >( leng2 ), &crcTmp1 );
			return static_cast< uint32_t >( crcTmp1 );
		}
};


template< CRC_UINT64 T >
uint32_t mycrc32( uint32_t crc, const uint8_t* block,uint32_t leng )
{
	return LizardCrc< T >::mycrc32( crc, block, leng );
}


template< CRC_UINT64 T >
uint32_t mycrc32_combine( uint32_t crc1, uint32_t crc2, uint32_t leng2 ) 
{
    return LizardCrc< T >::mycrc32_combine( crc1, crc2, leng2 );
}


#endif
