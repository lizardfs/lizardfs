/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA.

   This file is part of MooseFS.

   MooseFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   MooseFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with MooseFS.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "config.h"

#include <inttypes.h>
#include <stdlib.h>
#include "MFSCommunication.h"

/* original crc32 code
uint32_t* crc32_generate(void) {
	uint32_t *res;
	uint32_t crc, poly, i, j;

	res = (uint32_t*)malloc(sizeof(uint32_t)*256);
	poly = CRC_POLY;
	for (i=0 ; i<256 ; i++) {
		crc=i;
		for (j=0 ; j<8 ; j++) {
			if (crc & 1) {
				crc = (crc >> 1) ^ poly;
			} else {
				crc >>= 1;
			}
		}
		res[i] = crc;
	}
	return res;
}

uint32_t crc32(uint32_t crc,uint8_t *block,uint32_t leng) {
	uint8_t c;
	static uint32_t *crc_table = NULL;

	if (crc_table==NULL) {
		crc_table = crc32_generate();
	}

	crc^=0xFFFFFFFF;
	while (leng>0) {
		c = *block++;
		leng--;
		crc = ((crc>>8) & 0x00FFFFFF) ^ crc_table[ (crc^c) & 0xFF ];
	}
	return crc^0xFFFFFFFF;
}
*/

#define FASTCRC 1

#ifdef FASTCRC
#define BYTEREV(w) (((w)>>24)+(((w)>>8)&0xff00)+(((w)&0xff00)<<8)+(((w)&0xff)<<24))
static uint32_t crc_table[4][256];
#else
static uint32_t crc_table[256];
#endif

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
#ifdef FASTCRC
#ifdef WORDS_BIGENDIAN
		crc_table[0][i] = BYTEREV(c);
#else /* little endian */
		crc_table[0][i] = c;
#endif
#else
		crc_table[i]=c;
#endif
	}

#ifdef FASTCRC
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
/*
		c = crc_table[0][i];
		crc_table[4][i] = BYTEREV(c);
		c = crc_table[0][c&0xff]^(c>>8);
		crc_table[1][i] = c;
		crc_table[5][i] = BYTEREV(c);
		c = crc_table[0][c&0xff]^(c>>8);
		crc_table[2][i] = c;
		crc_table[6][i] = BYTEREV(c);
		c = crc_table[0][c&0xff]^(c>>8);
		crc_table[3][i] = c;
		crc_table[7][i] = BYTEREV(c);
*/
	}
#endif
}

uint32_t mycrc32(uint32_t crc,const uint8_t *block,uint32_t leng) {
#ifdef FASTCRC
	const uint32_t *block4;
#endif

#ifdef FASTCRC
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
#else
#define CRC_ONE_BYTE crc = (crc>>8)^crc_table[(crc^(*block++))&0xff]
	crc^=0xFFFFFFFF;
	while (leng>=8) {
		CRC_ONE_BYTE;
		CRC_ONE_BYTE;
		CRC_ONE_BYTE;
		CRC_ONE_BYTE;
		CRC_ONE_BYTE;
		CRC_ONE_BYTE;
		CRC_ONE_BYTE;
		CRC_ONE_BYTE;
		leng-=8;
	}
	if (leng>0) do {
		CRC_ONE_BYTE;
	} while (--leng);
	return crc^0xFFFFFFFF;
#endif
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
