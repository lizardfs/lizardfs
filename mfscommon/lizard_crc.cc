/*
 2013-09-10 witekj@op.pl
 */
#include "lizard_crc.h"

CrcInterface* const CrcCalcCollection::crcCalculator[] = {
  CrcInterface::Create(PNG_POLYNOMIAL),
  CrcInterface::Create(INTEL_POLYNOMIAL)
};
