/*
   Copyright 2016 Skytechnology sp. z o.o.

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

#include "common/platform.h"

#include <gtest/gtest.h>

#include "mount/readahead_adviser.h"

TEST(ReadaheadTests, ReadSequential) {
	ReadaheadAdviser ra(1024);

	int window = 0;
	for (int i = 0; i < 32; ++i) {
		ra.feed(i * 65536, 65536);
		ASSERT_GE(ra.window(), window);
		window = ra.window();
	}
}

TEST(ReadaheadTests, ReadHoles) {
	ReadaheadAdviser ra(1024);

	int i = 0;
	for (; i < 8; ++i) {
		ra.feed(i * 65536, 65536 - 1000*i);
	}
	int window = ra.window();
	for (; i < 16; ++i) {
		ra.feed(i * 65536, 65536 - 1000*i);
		ASSERT_LE(ra.window(), window);
		window = ra.window();
	}
}

TEST(ReadaheadTests, ReadOverlapping) {
	ReadaheadAdviser ra(1024);

	int i = 0;
	for (;i < 8; ++i) {
		ra.feed(i * 65536, 65536 + 1000*i);
	}
	int window = ra.window();
	for (;i < 16; ++i) {
		ASSERT_LE(ra.window(), window);
		window = ra.window();
	}
}

TEST(ReadaheadTests, ReadSequentialThenHolesThenSequential) {
	ReadaheadAdviser ra(1024);

	int i = 0;
	int window = 0;
	for (; i < 16; ++i) {
		ra.feed(i * 65536, 65536);
		ASSERT_GE(ra.window(), window);
		window = ra.window();
	}

	for (; i < 20; ++i) {
		ra.feed(i * 65536, 65536 - 1000*i);
	}
	for (; i < 48; ++i) {
		ra.feed(i * 65536, 65536 - 1000*i);
		ASSERT_LE(ra.window(), window);
		window = ra.window();
	}

	for (; i < 52; ++i) {
		ra.feed(i * 65536, 65536);
	}
	for (; i < 64; ++i) {
		ra.feed(i * 65536, 65536);
		ASSERT_GE(ra.window(), window);
		window = ra.window();
	}
}
