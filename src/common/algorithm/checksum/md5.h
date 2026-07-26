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

#pragma once

#include "common/platform.h"

#include <inttypes.h>
#include <array>
#include <string>
#include <vector>

typedef struct _md5ctx {
	uint32_t state[4];
	uint32_t count[2];
	uint8_t buffer[64];
} md5ctx;

void md5_init(md5ctx *ctx);
void md5_update(md5ctx *ctx,const uint8_t *buff,uint32_t leng);
void md5_final(uint8_t digest[16],md5ctx *ctx);
std::array<uint8_t, 16> md5_challenge_response(const std::array<uint8_t, 32>& challenge,
		std::string data);

int md5_parse(std::vector<uint8_t> &password_digest, const char *in_md5_data);
