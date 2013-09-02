#include <crc_sse4.h>

uint32_t CrcContainer::mycrc32( uint32_t crc, const uint8_t* block,uint32_t leng ) {
    crcutil_interface::CRC* crcCalc = CrcContainer::getCrc();

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

uint32_t CrcContainer::mycrc32_combine( uint32_t crc1, uint32_t crc2, uint32_t leng2 ) {

    crcutil_interface::CRC* crcCalc = CrcContainer::getCrc();
    CRC_UINT64 crcTmp1 = static_cast< CRC_UINT64 >( crc1 );
    crcCalc->Concatenate( static_cast< CRC_UINT64 >( crc2 ), 0, static_cast< CRC_UINT64 >( leng2 ), &crcTmp1 );
    return static_cast< uint32_t >( crcTmp1 );
}


map< __pid_t, crcutil_interface::CRC* > CrcContainer::m_CrcBag;

crcutil_interface::CRC* CrcContainer::createInstance()
{
    bool isSse = crcutil_interface::CRC::IsSSE42Available();
    crcutil_interface::CRC* crc = crcutil_interface::CRC::Create(
                                  CRC_POLY, 0, 32, true, 0, 0, 0,
                                  isSse , NULL );

    CrcContainer::m_CrcBag.insert( make_pair( getpid(), crc ) );
    return crc;
}

crcutil_interface::CRC* CrcContainer::getCrc() {
    map< __pid_t, crcutil_interface::CRC* >::iterator iter =
            CrcContainer::m_CrcBag.find( getpid() );
    if( iter != CrcContainer::m_CrcBag.end() ) {
        return iter->second;
    }

    return createInstance();
}
