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

#ifndef _MD5_H_
#define _MD5_H_

#include <inttypes.h>

typedef struct _md5ctx {
	uint32_t state[4];
	uint32_t count[2];
	uint8_t buffer[64];
} md5ctx;

void md5_init(md5ctx *ctx);
void md5_update(md5ctx *ctx,const uint8_t *buff,uint32_t leng);
void md5_final(uint8_t digest[16],md5ctx *ctx);

#endif
