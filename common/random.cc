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

#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <inttypes.h>

static uint8_t i,j;
static uint8_t p[256];

int rnd_init(void) {
	uint8_t key[64],vkey[64];
	register uint8_t x;
	uint16_t l;

	srandom(time(NULL));
	for (l=0 ; l<64 ; l++) {
		key[l] = random();
		vkey[l] = random();
	}
	for (l=0 ; l<256 ; l++) {
		p[l]=l;
	}
	for (l=0 ; l<768 ; l++) {
		i = l&0xFF;
		x = j+p[i]+key[l%64];
		j = p[x];
		x = p[i];
		p[i] = p[j];
		p[j] = x;
	}
	for (l=0 ; l<768 ; l++) {
		i = l&0xFF;
		x = j+p[i]+vkey[l%64];
		j = p[x];
		x = p[i];
		p[i] = p[j];
		p[j] = x;
	}
	i = 0;
	return 0;
}

#define RND_RC4_STEP(result) { \
	register uint8_t x; \
	x = j+p[i]; \
	j = p[x]; \
	x = p[j]; \
	x = p[x]+1; \
	result = p[x]; \
	x = p[i]; \
	p[i] = p[j]; \
	p[j] = x; \
	i++; \
}

uint8_t rndu8() {
	uint8_t r;
	RND_RC4_STEP(r);
	return r;
}

uint32_t rndu32() {
	uint32_t res;
	uint8_t *r = (uint8_t*)&res;
	RND_RC4_STEP(r[0]);
	RND_RC4_STEP(r[1]);
	RND_RC4_STEP(r[2]);
	RND_RC4_STEP(r[3]);
	return res;
}

uint64_t rndu64() {
	uint64_t res;
	uint8_t *r = (uint8_t*)&res;
	RND_RC4_STEP(r[0]);
	RND_RC4_STEP(r[1]);
	RND_RC4_STEP(r[2]);
	RND_RC4_STEP(r[3]);
	RND_RC4_STEP(r[4]);
	RND_RC4_STEP(r[5]);
	RND_RC4_STEP(r[6]);
	RND_RC4_STEP(r[7]);
	return res;
}

uint64_t rndu64_ranged(uint64_t range) {
	uint64_t max,r;
	r = rndu64();
	if (range==0) {
		return r;
	}
	max = -(((uint64_t)-range) % range);
	if (max) {
		while (r>=max) {
			r = rndu64();
		}
	}
	return r%range;
}

uint32_t rndu32_ranged(uint32_t range) {
	uint32_t max,r;
	r = rndu32();
	if (range==0) {
		return r;
	}
	max = -(((uint32_t)-range) % range);
	if (max) {
		while (r>=max) {
			r = rndu32();
		}
	}
	return r%range;
}
