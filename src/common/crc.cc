/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"
#include "common/crc.h"

#include <inttypes.h>
#include <stdlib.h>

#include "common/MFSCommunication.h"

#ifndef ENABLE_CRC

static uint32_t FAKE_CRC = 0xFEDCBA98;

uint32_t mycrc32(uint32_t, const uint8_t*, uint32_t) {
	return FAKE_CRC;
}

uint32_t mycrc32_combine(uint32_t, uint32_t, uint32_t) {
	return FAKE_CRC;
}

void mycrc32_init(void) {
}

#else // ENABLE_CRC

/*
 * CRC implementation from crcutil supports only little endian machines.
 * For big endian we provide legacy implementation.
 */
#ifdef HAVE_CRCUTIL
#include <generic_crc.h>

static crcutil::GenericCrc<uint64_t, uint64_t, uint64_t, 4> gCrc(CRC_POLY, 32, true);

uint32_t mycrc32(uint32_t crc, const uint8_t *block, uint32_t leng) {
	return gCrc.CrcDefault(block, leng, crc);
}

uint32_t mycrc32_combine(uint32_t crc1, uint32_t crc2, uint32_t leng2) {
	return gCrc.Base().Concatenate(crc1, crc2, leng2);
}

void mycrc32_init(void) {
	// This implementation does not need any initialization
}

#else // Use old code, which supports both big endian and little endian

#define BYTEREV(w) (((w)>>24)+(((w)>>8)&0xff00)+(((w)&0xff00)<<8)+(((w)&0xff)<<24))
static uint32_t crc_table[4][256];

void crc_generate_main_tables(void) {
	uint32_t c,poly,i;

	poly = CRC_POLY;
	for (i=0; i<256; i++) {
		c=i;
		c = (c&1) ? (poly^(c>>1)) : (c>>1);
		c = (c&1) ? (poly^(c>>1)) : (c>>1);
		c = (c&1) ? (poly^(c>>1)) : (c>>1);
		c = (c&1) ? (poly^(c>>1)) : (c>>1);
		c = (c&1) ? (poly^(c>>1)) : (c>>1);
		c = (c&1) ? (poly^(c>>1)) : (c>>1);
		c = (c&1) ? (poly^(c>>1)) : (c>>1);
		c = (c&1) ? (poly^(c>>1)) : (c>>1);
#ifdef WORDS_BIGENDIAN
		crc_table[0][i] = BYTEREV(c);
#else /* little endian */
		crc_table[0][i] = c;
#endif
	}

	for (i=0; i<256; i++) {
#ifdef WORDS_BIGENDIAN
		c = crc_table[0][i];
		c = crc_table[0][(c>>24)]^(c<<8);
		crc_table[1][i] = c;
		c = crc_table[0][(c>>24)]^(c<<8);
		crc_table[2][i] = c;
		c = crc_table[0][(c>>24)]^(c<<8);
		crc_table[3][i] = c;
#else /* little endian */
		c = crc_table[0][i];
		c = crc_table[0][c&0xff]^(c>>8);
		crc_table[1][i] = c;
		c = crc_table[0][c&0xff]^(c>>8);
		crc_table[2][i] = c;
		c = crc_table[0][c&0xff]^(c>>8);
		crc_table[3][i] = c;
#endif
	}
}

uint32_t mycrc32(uint32_t crc,const uint8_t *block,uint32_t leng) {
	const uint32_t *block4;
#ifdef WORDS_BIGENDIAN
#define CRC_REORDER crc=(BYTEREV(crc))^0xFFFFFFFF
#define CRC_ONE_BYTE crc = crc_table[0][(crc >> 24) ^ *block++] ^ (crc << 8)
#define CRC_FOUR_BYTES crc ^= *block4++; crc = crc_table[0][crc & 0xff] ^ crc_table[1][(crc >> 8) & 0xff] ^ crc_table[2][(crc >> 16) & 0xff] ^ crc_table[3][crc >> 24]
#else /* little endian */
#define CRC_REORDER crc^=0xFFFFFFFF
#define CRC_ONE_BYTE crc = crc_table[0][(crc ^ *block++) & 0xFF] ^ (crc >> 8)
#define CRC_FOUR_BYTES crc ^= *block4++; crc = crc_table[3][crc & 0xff] ^ crc_table[2][(crc >> 8) & 0xff] ^ crc_table[1][(crc >> 16) & 0xff] ^ crc_table[0][crc >> 24]
#endif
	CRC_REORDER;
	while (leng && ((unsigned long)block & 3)) {
		CRC_ONE_BYTE;
		leng--;
	}
	block4 = (const uint32_t*)block;
	while (leng>=32) {
		CRC_FOUR_BYTES;
		CRC_FOUR_BYTES;
		CRC_FOUR_BYTES;
		CRC_FOUR_BYTES;
		CRC_FOUR_BYTES;
		CRC_FOUR_BYTES;
		CRC_FOUR_BYTES;
		CRC_FOUR_BYTES;
		leng-=32;
	}
	while (leng>=4) {
		CRC_FOUR_BYTES;
		leng-=4;
	}
	block = (const uint8_t*)block4;
	if (leng) do {
		CRC_ONE_BYTE;
	} while (--leng);
	CRC_REORDER;
	return crc;
}

/* crc_combine */

static uint32_t crc_combine_table[32][4][256];

static void crc_matrix_square(uint32_t sqr[32], uint32_t m[32]) {
	uint32_t i,j,s,v;
	for (i=0; i<32; i++) {
		for (j=0,s=0,v=m[i] ; v && j<32 ; j++, v>>=1) {
			if (v&1) {
				s^=m[j];
			}
		}
		sqr[i] = s;
	}
}

static void crc_generate_combine_tables(void) {
	uint32_t i,j,k,l,sum;
	uint32_t m1[32],m2[32],*mc,*m;
	m1[0]=CRC_POLY;
	j=1;
	for (i=1 ; i<32 ; i++) {
		m1[i]=j;
		j<<=1;
	}
	crc_matrix_square(m2,m1); // 1 bit -> 2 bits
	crc_matrix_square(m1,m2); // 2 bits -> 4 bits

	for (i=0 ; i<32 ; i++) {
		if (i&1) {
			crc_matrix_square(m1,m2);
			mc = m1;
		} else {
			crc_matrix_square(m2,m1);
			mc = m2;
		}
		for (j=0 ; j<4 ; j++) {
			for (k=0 ; k<256 ; k++) {
				sum = 0;
				l=k;
				m=mc+(j*8);
				while (l) {
					if (l&1) {
						sum ^= *m;
					}
					l>>=1;
					m++;
				}
				crc_combine_table[i][j][k]=sum;
			}
		}
	}
}

uint32_t mycrc32_combine(uint32_t crc1, uint32_t crc2, uint32_t leng2) {
	uint8_t i;

	/* add leng2 zeros to crc1 */
	i=0;
	while (leng2) {
		if (leng2&1) {
			crc1 = crc_combine_table[i][3][(crc1>>24)] \
			     ^ crc_combine_table[i][2][(crc1>>16)&0xFF] \
			     ^ crc_combine_table[i][1][(crc1>>8)&0xFF] \
			     ^ crc_combine_table[i][0][crc1&0xFF];
		}
		i++;
		leng2>>=1;
	};
	/* then combine crc1 and crc2 as output */
	return crc1^crc2;
}

void mycrc32_init(void) {
	crc_generate_main_tables();
	crc_generate_combine_tables();
}

#endif // HAVE_CRCUTIL

#endif // ENABLE_CRC

void recompute_crc_if_block_empty(uint8_t* block, uint32_t& crc) {
	// If both block and crcBuffer consist only of zeros recompute the crc
	if (crc == 0) {
		if (block[0] == 0 && !memcmp(block, block + 1, MFSBLOCKSIZE - 1)) {
			static uint32_t emptyBlockCrc = mycrc32_zeroblock(0, MFSBLOCKSIZE);
			crc = emptyBlockCrc;
		}
	}
}
