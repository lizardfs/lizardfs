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
//const CRC_UINT64 INTEL_CRC = 0x82F63B78;


class CrcContainer
{
    private:
        static map< __pid_t, crcutil_interface::CRC* > m_CrcBag;
        static crcutil_interface::CRC* createInstance();

    public:
        static uint32_t mycrc32( uint32_t crc, const uint8_t* block,uint32_t leng );
        static uint32_t mycrc32_combine( uint32_t crc1, uint32_t crc2, uint32_t leng2 );
        static crcutil_interface::CRC* getCrc();
};


#endif
