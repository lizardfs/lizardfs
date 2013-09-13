/*
 * Copyrights 2013 Skytechnology sp. z o.o.
 */

#include <gtest/gtest.h>

#include "mfscommon/charts.h"
#include "mfscommon/crc.h"

//Private memebers, unlisted in .h file:
void charts_fill_crc(uint8_t *buff,uint32_t leng);

static uint8_t png_1x1[] = {
			137, 80, 78, 71, 13, 10, 26, 10, // signature
			//+8 	12
			0, 0, 0, 13, 'I', 'H', 'D', 'R', // IHDR chunk
			0, 0, 0, 1, // width
			0, 0, 0, 1, // height
			8, 4, 0, 0, 0, // 8bits, grey alpha, def. compression, def. filters, no interlace
			'C', 'R', 'C', '#', // CRC == 0xb5, 0x1c, 0x0c, 0x02
			//+25
			0, 0, 0, 11, 'I', 'D', 'A', 'T', // IDAT chunk
			0x08, 0xd7, 0x63, 0x60, 0x60, 0x00,
			0x00, 0x00, 0x03, 0x00, 0x01,
			'C', 'R', 'C', '#', // CRC == 0x20, 0xd5, 0x94, 0xc7,
			//+23 	23
			0, 0, 0, 0, 'I', 'E', 'N', 'D', // IEND chunk
			'C', 'R', 'C', '#' // CRC == 0xae, 0x42, 0x60, 0x82
			//+12:
			};

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(TestCharts, ChartsFillCrc) {
	mycrc32_init();
	charts_fill_crc(png_1x1, sizeof(png_1x1));
	EXPECT_EQ(0, memcmp(png_1x1 + 8 + 25 - 4, "\xB5\x1C\x0C\x02", 4));
	EXPECT_EQ(0, memcmp(png_1x1 + 8 + 25 + 23 - 4, "\x20\xD5\x94\xC7", 4));
	EXPECT_EQ(0, memcmp(png_1x1 + 8 + 25 + 23 + 12 - 4, "\xAE\x42\x60\x82", 4));
}
