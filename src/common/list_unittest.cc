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

#include "common/platform.h"
#include "common/list.h"

#include <gtest/gtest.h>

struct SophisticatedList {
	char dummy1;
	uint32_t dummy2;
	SophisticatedList* next;
};

TEST(ListTests, ListLength) {
	SophisticatedList l1;
	EXPECT_EQ(0u, list_length((SophisticatedList*) NULL));
	l1.next = NULL;
	EXPECT_EQ(1u, list_length(&l1));
	SophisticatedList l2;
	l2.next = &l1;
	EXPECT_EQ(2u, list_length(&l2));
}
