/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * mycrc32.cc
 *
 *  Created on: 05-07-2013
 *      Author: Marcin Sulikowski
 */

#include "common/platform.h"

#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <boost/scoped_array.hpp>

#include "common/crc.h"

int main() {
	const size_t MAX_BUFFER_SIZE = 1024 * 1024 * 128 + 1;
	boost::scoped_array<uint8_t> buffer(new uint8_t[MAX_BUFFER_SIZE]);

	uint32_t bytesRead = fread(buffer.get(), 1, MAX_BUFFER_SIZE - 1, stdin);
	if (bytesRead == MAX_BUFFER_SIZE - 1) {
		fprintf(stderr, "Input too large\n");
		return 1;
	}
	buffer[bytesRead] = 0;

	mycrc32_init();
	uint32_t crc = mycrc32(0, buffer.get(), bytesRead);
	printf("0x%08x\n", crc);
	return 0;
}
