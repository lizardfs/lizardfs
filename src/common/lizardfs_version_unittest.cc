/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

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
#include "common/lizardfs_version.h"

#include <gtest/gtest.h>

TEST(LizardFsVersionTests, LizardFsVersion) {
	EXPECT_EQ(0x01061BU, lizardfsVersion(1, 6, 27));
	EXPECT_EQ(0x01061CU, lizardfsVersion(1, 6, 28));
	EXPECT_EQ(0x01071BU, lizardfsVersion(1, 7, 27));
	EXPECT_EQ(0x02061BU, lizardfsVersion(2, 6, 27));
}
