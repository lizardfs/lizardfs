/*
 * mycrc32.cc
 *
 *  Created on: 05-07-2013
 *      Author: Marcin Sulikowski
 */

#include "config.h"
#include "devtools/mycrc32/mycrc32.h"

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
