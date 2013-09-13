#include "../../../mfscommon/png.h"
#include "../../../mfscommon/crc.h"
#include <gtest/gtest.h>

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

static uint8_t png_1x1[] = {
    137, 80, 78, 71, 13, 10, 26, 10, // signature
//+8
    0, 0, 0, 13, 'I', 'H', 'D', 'R', // IHDR chunk
    0, 0, 0, 1,                      // width
    0, 0, 0, 1,                      // height
    8, 4, 0, 0, 0,                   // 8bits, grayscale with alpha color mode, default compression, default filters, no interlace
    0xb5, 0x1c, 0x0c, 0x02,          // CRC
//+25
    0, 0, 0, 11, 'I', 'D', 'A', 'T',   // IDAT chunk
    0x08, 0xd7, 0x63, 0x60, 0x60, 0x00,
    0x00, 0x00, 0x03, 0x00, 0x01,
    0x20, 0xd5, 0x94, 0xc7,           // CRC
//+23
    0, 0, 0, 0, 'I', 'E', 'N', 'D',   // IEND chunk
    0xae, 0x42, 0x60, 0x82            // CRC
//+12:
};

TEST(TestPng, PngIteratingOverChunks) {
    uint8_t *chunk = png_first_chunk(png_1x1);
    EXPECT_EQ(chunk,png_1x1 + 8);
    chunk = png_next_chunk(chunk);
    EXPECT_EQ(chunk,png_1x1 + 8 + 25);
    chunk = png_next_chunk(chunk);
    EXPECT_EQ(chunk,png_1x1 + 8 + 25 + 23);
    chunk = png_next_chunk(chunk);
    EXPECT_EQ(chunk,png_1x1 + sizeof(png_1x1));
}

TEST(TestPng,PngIteratingOverChunksConst) {
    const uint8_t *chunk = png_first_chunk((const uint8_t *)png_1x1);
    EXPECT_EQ(chunk,png_1x1 + 8);
    chunk = png_next_chunk(chunk);
    EXPECT_EQ(chunk,png_1x1 + 8 + 25);
    chunk = png_next_chunk(chunk);
    EXPECT_EQ(chunk,png_1x1 + 8 + 25 + 23);
    chunk = png_next_chunk(chunk);
    EXPECT_EQ(chunk,png_1x1 + sizeof(png_1x1));
}

TEST(TestPng,PngChunkSize) {
    EXPECT_EQ(13,png_chunk_get_data_size(png_1x1 + 8));
    EXPECT_EQ(11,png_chunk_get_data_size(png_1x1 + 8 + 25));
    EXPECT_EQ(0,png_chunk_get_data_size(png_1x1 + 8 + 25 + 23));
}

TEST(TestPng, PngChunkVerifyLimits) {
    const uint8_t *chunk = png_1x1 + 8;
    for (unsigned span = 0;span < sizeof(png_1x1);++span) {
      if ( span < 8 + 25) {
          EXPECT_NE(0,png_chunk_verify_limits(chunk,png_1x1+span));
      } else {
          EXPECT_EQ(0,png_chunk_verify_limits(chunk,png_1x1+span));
      }
    }
    chunk += 25;
    for (unsigned span = 0;span < sizeof(png_1x1);++span) {
      if ( span < 8 + 25 + 23) {
          EXPECT_NE(0,png_chunk_verify_limits(chunk,png_1x1+span));
      } else {
          EXPECT_EQ(0,png_chunk_verify_limits(chunk,png_1x1+span));
      }
    }
    chunk += 23;
    for (unsigned span = 0;span < sizeof(png_1x1);++span) {
      if ( span < 8 + 25 + 23 + 12) {
          EXPECT_NE(0,png_chunk_verify_limits(chunk,png_1x1+span));
      } else {
          EXPECT_EQ(0,png_chunk_verify_limits(chunk,png_1x1+span));
      }
    }
}

TEST(tst_png, PngChunkCrcOffset) {
    EXPECT_EQ(png_1x1 + 8 + 25 - 4,png_1x1 + 8 + png_chunk_get_crc_offset(png_1x1 + 8));
    EXPECT_EQ(png_1x1 + 8 + 25 + 23 - 4,png_1x1 + 8 + 25 + png_chunk_get_crc_offset(png_1x1 + 8 + 25));
    EXPECT_EQ(png_1x1 + 8 + 25 + 23 + 12 - 4,png_1x1 + 8 + 25 + 23 + png_chunk_get_crc_offset(png_1x1 + 8 + 25 + 23));
}

TEST(tst_png, PngChunkCrcGet) {
    EXPECT_EQ(0xB51C0C02,png_chunk_get_crc(png_1x1 + 8));
    EXPECT_EQ(0x20D594C7,png_chunk_get_crc(png_1x1 + 8 + 25));
    EXPECT_EQ(0xAE426082,png_chunk_get_crc(png_1x1 + 8 + 25 + 23));
}

TEST(tst_png, PngChunkCrcCompute) {
    EXPECT_EQ(0xB51C0C02,png_chunk_compute_crc(png_1x1 + 8));
    EXPECT_EQ(0x20D594C7,png_chunk_compute_crc(png_1x1 + 8 + 25));
    EXPECT_EQ(0xAE426082,png_chunk_compute_crc(png_1x1 + 8 + 25 + 23));
}

TEST(tst_png, PngChunkCrcVerify) {
    EXPECT_EQ(0,png_chunk_verify_crc(png_1x1 + 8));
    EXPECT_EQ(0,png_chunk_verify_crc(png_1x1 + 8 + 25));
    EXPECT_EQ(0,png_chunk_verify_crc(png_1x1 + 8 + 25 + 23));

    png_1x1[8 + 25 - 1] += 1;
    EXPECT_NE(0,png_chunk_verify_crc(png_1x1 + 8));
    png_1x1[8 + 25 - 1] -= 1;
    png_1x1[8 + 25 + 23 -1] += 1;
    EXPECT_NE(0,png_chunk_verify_crc(png_1x1 + 8 + 25));
    png_1x1[8 + 25 + 23 -1] -= 1;
    png_1x1[8 + 25 + 23 + 12 -1] += 1;
    EXPECT_NE(0,png_chunk_verify_crc(png_1x1 + 8 + 25 + 23));
    png_1x1[8 + 25 + 23 + 12 -1] -= 1;
}

TEST(tst_png, PngChunkCrcSet) {
    png_chunk_set_crc(png_1x1 + 8,0x12345678);
    EXPECT_EQ(0,memcmp("\x12\x34\x56\x78",png_1x1 + 8 + 25 - 4, 4));
    memcpy(png_1x1 + 8 + 25 - 4,"\xB5\x1C\x0C\x02",4);
}

TEST(tst_png, PngChunkCrcUpdate) {
    memcpy(png_1x1 + 8 + 25 - 4,"\xFE\xFE\xFE\xFE",4);
    png_chunk_update_crc(png_1x1 + 8);
    EXPECT_EQ(0,memcmp("\xB5\x1C\x0C\x02",png_1x1 + 8 + 25 - 4, 4));
    memcpy(png_1x1 + 8 + 25 - 4,"\xB5\x1C\x0C\x02",4);
}
