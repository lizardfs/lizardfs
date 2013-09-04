#include <crc_sse4.h>

CrcContainer::CRC_BAG_TYPE CrcContainer::m_CrcBag;

crcutil_interface::CRC* CrcContainer::createInstance( const CRC_UINT64& pPoly ){
    bool isSse = crcutil_interface::CRC::IsSSE42Available();
    crcutil_interface::CRC* crc = crcutil_interface::CRC::Create(
                                  pPoly, 0, 32, true, 0, 0, 0,
                                  isSse , NULL );
	CrcContainer::m_CrcBag.insert( make_pair( make_pair( getpid(), pPoly ) , crc ) );

    return crc;
}


crcutil_interface::CRC* CrcContainer::getCrc( const CRC_UINT64& pPoly ) {
    CRC_BAG_TYPE::iterator iter = CrcContainer::m_CrcBag.find( make_pair( getpid(), pPoly )  );
    if( iter != CrcContainer::m_CrcBag.end() ) {
        return iter->second;
    }
    return createInstance( pPoly );
}
