#include "lizard_crc.h"
#include "crc.h"

uint32_t mycrc32(uint32_t crc, const uint8_t *block, uint32_t leng) {
    return LizardCrc< PNG_POLY_INDEX >::crc32(crc, block, leng);
}

uint32_t mycrc32_combine(uint32_t crc1, uint32_t crc2, uint32_t leng2) {
    return LizardCrc< PNG_POLY_INDEX >::crc32Combine(crc1, crc2, leng2);
}
