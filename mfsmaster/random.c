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


int rndinit(FILE *msgfd) {
	uint8_t key[64],vkey[64];
	register uint8_t x;
	uint16_t l;

	(void)msgfd;
	srandom(time(NULL));
	for (l=0 ; l<64 ; l++) {
		key[l] = random();
		vkey[l] = random();
	}
	for (l=0 ; l<256 ; l++) {
		p[l]=l;
	}
	for (l=0 ; l<768 ; l++) {
		i = l;
		x = j+p[i]+key[l%64];
		j = p[x];
		x = p[i];
		p[i] = p[j];
		p[j] = x;
	}
	for (l=0 ; l<768 ; l++) {
		i = l;
		x = j+p[i]+vkey[l%64];
		j = p[x];
		x = p[i];
		p[i] = p[j];
		p[j] = x;
	}
	i = 0;
	return 0;
}

uint8_t rndu8() {
	register uint8_t r,x;
	x = j+p[i];
	j = p[x];
	x = p[j];
	x = p[x]+1;
	r = p[x];
	x = p[i];
	p[i] = p[j];
	p[j] = x;
	i++;
	return r;
}

uint32_t rndu32() {
	register uint8_t x;
	uint8_t r[4];

	x = j+p[i];
	j = p[x];
	x = p[j];
	x = p[x]+1;
	r[0] = p[x];
	x = p[i];
	p[i] = p[j];
	p[j] = x;
	i++;

	x = j+p[i];
	j = p[x];
	x = p[j];
	x = p[x]+1;
	r[1] = p[x];
	x = p[i];
	p[i] = p[j];
	p[j] = x;
	i++;

	x = j+p[i];
	j = p[x];
	x = p[j];
	x = p[x]+1;
	r[2] = p[x];
	x = p[i];
	p[i] = p[j];
	p[j] = x;
	i++;

	x = j+p[i];
	j = p[x];
	x = p[j];
	x = p[x]+1;
	r[3] = p[x];
	x = p[i];
	p[i] = p[j];
	p[j] = x;
	i++;

	return *((uint32_t*)(r));
}
