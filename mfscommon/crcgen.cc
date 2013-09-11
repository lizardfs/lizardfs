/*
 2013-09-10 witekj@op.pl
 */
#include "crcgen.h"

CrcIfc* const CrcCalcWrap::m_CrcCalcArray[] = {
                                                CrcIfc::Create( ZLIB_POLY ),
                                                CrcIfc::Create( INTEL_POLY )
                                               };
