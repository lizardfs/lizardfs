/*
   Copyright 2008 Gemius SA.

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

#include <inttypes.h>
#include <stdlib.h>
#include "MFSCommunication.h"

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
