/* 2013/09/31 witekj@op.pl
 *
*/

#include <crc_sse4.h>

pthread_mutex_t CrcPool::m_CrcPoolLock = PTHREAD_MUTEX_INITIALIZER;

CrcPool::CRC_BAG_TYPE CrcPool::m_CrcBag;

crcutil_interface::CRC* CrcPool::createInstance( const CRC_UINT64& pPoly ){
    bool isSse = crcutil_interface::CRC::IsSSE42Available();
    crcutil_interface::CRC* crc = crcutil_interface::CRC::Create(
                                  pPoly, 0, 32, true, 0, 0, 0,
                                  isSse , NULL );
    CrcPool::m_CrcBag.insert( make_pair( make_pair( pthread_self(), pPoly ) , crc ) );

    return crc;
}

#define CRC_POOL_LOCK \
if ( pthread_mutex_lock( &m_CrcPoolLock ) < 0 ) { \
    printf( "lock error: %s\n", __FUNCTION__ ); \
    return NULL; \
}


#define CRC_POOL_UNLOCK \
if ( pthread_mutex_unlock( &m_CrcPoolLock ) < 0 ) { \
    printf("unlock error: %s\n", __FUNCTION__ ); \
}

crcutil_interface::CRC* CrcPool::getCrc( const CRC_UINT64& pPoly ) {
    CRC_POOL_LOCK
    CRC_BAG_TYPE::iterator iter = CrcPool::m_CrcBag.find( make_pair( pthread_self(), pPoly )  );
    if( iter != CrcPool::m_CrcBag.end() ) {
        CRC_POOL_UNLOCK
        return iter->second;
    }

    crcutil_interface::CRC* crc = createInstance( pPoly );
    CRC_POOL_UNLOCK
    return crc;
}


