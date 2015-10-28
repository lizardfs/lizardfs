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
#include "master/quota_database.h"

#include <gtest/gtest.h>

#define EXPECT_ENTRY_EQ(entry, ivalue, svalue, isoft, ihard, ssoft, shard) \
		ASSERT_NE(nullptr, entry); \
		EXPECT_EQ(ivalue, (*entry)[(int)QuotaRigor::kUsed][(int)QuotaResource::kInodes]); \
		EXPECT_EQ(svalue, (*entry)[(int)QuotaRigor::kUsed][(int)QuotaResource::kSize]); \
		EXPECT_EQ(isoft, (*entry)[(int)QuotaRigor::kSoft][(int)QuotaResource::kInodes]); \
		EXPECT_EQ(ihard, (*entry)[(int)QuotaRigor::kHard][(int)QuotaResource::kInodes]); \
		EXPECT_EQ(ssoft, (*entry)[(int)QuotaRigor::kSoft][(int)QuotaResource::kSize]); \
		EXPECT_EQ(shard, (*entry)[(int)QuotaRigor::kHard][(int)QuotaResource::kSize]);


TEST(QuotaDatabaseTests, SetGetQuota) {
	QuotaDatabase database;
	ASSERT_EQ(nullptr, database.get(QuotaOwnerType::kUser, 999));

	database.set(QuotaOwnerType::kUser, 999, QuotaRigor::kSoft, QuotaResource::kInodes, 100);
	ASSERT_EQ(nullptr, database.get(QuotaOwnerType::kGroup, 999));
	EXPECT_ENTRY_EQ(database.get(QuotaOwnerType::kUser, 999), 0U, 0U, 100U, 0U, 0U, 0U);

	database.set(QuotaOwnerType::kUser, 999, QuotaRigor::kHard, QuotaResource::kInodes, 200);
	ASSERT_EQ(nullptr, database.get(QuotaOwnerType::kGroup, 999));
	EXPECT_ENTRY_EQ(database.get(QuotaOwnerType::kUser, 999), 0U, 0U, 100U, 200U, 0U, 0U);

	database.set(QuotaOwnerType::kUser, 999, QuotaRigor::kSoft, QuotaResource::kSize, 300);
	ASSERT_EQ(nullptr, database.get(QuotaOwnerType::kGroup, 999));
	EXPECT_ENTRY_EQ(database.get(QuotaOwnerType::kUser, 999), 0U, 0U, 100U, 200U, 300U, 0U);

	database.set(QuotaOwnerType::kUser, 999, QuotaRigor::kHard, QuotaResource::kSize, 400);
	ASSERT_EQ(nullptr, database.get(QuotaOwnerType::kGroup, 999));
	EXPECT_ENTRY_EQ(database.get(QuotaOwnerType::kUser, 999), 0U, 0U, 100U, 200U, 300U, 400U);
}


TEST(QuotaDatabaseTests, RemoveQuota) {
	QuotaDatabase database;
	database.set(QuotaOwnerType::kGroup, 999, QuotaRigor::kSoft, QuotaResource::kInodes, 100);
	database.set(QuotaOwnerType::kGroup, 999, QuotaRigor::kHard, QuotaResource::kInodes, 200);
	EXPECT_ENTRY_EQ(database.get(QuotaOwnerType::kGroup, 999), 0U, 0U, 100U, 200U, 0U, 0U);

	database.remove(QuotaOwnerType::kGroup, 999, QuotaRigor::kSoft, QuotaResource::kInodes);
	EXPECT_ENTRY_EQ(database.get(QuotaOwnerType::kGroup, 999), 0U, 0U, 0U, 200U, 0U, 0U);

	database.remove(QuotaOwnerType::kGroup, 999, QuotaRigor::kHard, QuotaResource::kInodes);
	EXPECT_EQ(database.get(QuotaOwnerType::kGroup, 999), nullptr);
}

TEST(QuotaDatabaseTests, IsExceeded) {
	QuotaDatabase database;

	database.set(QuotaOwnerType::kUser, 999, QuotaRigor::kSoft, QuotaResource::kInodes, 100);
	database.set(QuotaOwnerType::kUser, 999, QuotaRigor::kHard, QuotaResource::kInodes, 200);
	database.set(QuotaOwnerType::kGroup, 999, QuotaRigor::kSoft, QuotaResource::kInodes, 300);
	database.set(QuotaOwnerType::kGroup, 999, QuotaRigor::kHard, QuotaResource::kInodes, 400);
	database.set(QuotaOwnerType::kUser, 999, QuotaRigor::kSoft, QuotaResource::kSize, 1000);
	database.set(QuotaOwnerType::kUser, 999, QuotaRigor::kHard, QuotaResource::kSize, 2000);
	database.set(QuotaOwnerType::kGroup, 999, QuotaRigor::kSoft, QuotaResource::kSize, 3000);
	database.set(QuotaOwnerType::kGroup, 999, QuotaRigor::kHard, QuotaResource::kSize, 4000);

	database.update(QuotaOwnerType::kUser, 999, QuotaRigor::kUsed, QuotaResource::kInodes, 150);
	database.update(QuotaOwnerType::kGroup, 999, QuotaRigor::kUsed, QuotaResource::kInodes, 150);
	EXPECT_TRUE(database.exceeds(QuotaOwnerType::kUser, 999, QuotaRigor::kSoft, {{QuotaResource::kInodes, 1}}));
	EXPECT_FALSE(database.exceeds(QuotaOwnerType::kGroup, 999, QuotaRigor::kSoft, {{QuotaResource::kInodes, 1}}));
	EXPECT_FALSE(database.exceeds(QuotaOwnerType::kUser, 999, QuotaRigor::kHard, {{QuotaResource::kInodes, 1}}));
	EXPECT_FALSE(database.exceeds(QuotaOwnerType::kGroup, 999, QuotaRigor::kHard, {{QuotaResource::kInodes, 1}}));

	EXPECT_FALSE(database.exceeds(QuotaOwnerType::kUser, 999, QuotaRigor::kSoft, {{QuotaResource::kSize, 1}}));
	EXPECT_FALSE(database.exceeds(QuotaOwnerType::kGroup, 999, QuotaRigor::kSoft, {{QuotaResource::kSize, 1}}));
	EXPECT_FALSE(database.exceeds(QuotaOwnerType::kUser, 999, QuotaRigor::kHard, {{QuotaResource::kSize, 1}}));
	EXPECT_FALSE(database.exceeds(QuotaOwnerType::kGroup, 999, QuotaRigor::kHard, {{QuotaResource::kSize, 1}}));

	database.update(QuotaOwnerType::kUser, 1, QuotaRigor::kUsed, QuotaResource::kInodes, 270);
	database.update(QuotaOwnerType::kGroup, 999, QuotaRigor::kUsed, QuotaResource::kInodes, 270);
	EXPECT_TRUE(database.exceeds(QuotaOwnerType::kUser, 999, QuotaRigor::kSoft, {{QuotaResource::kInodes, 1}}));
	EXPECT_TRUE(database.exceeds(QuotaOwnerType::kGroup, 999, QuotaRigor::kSoft, {{QuotaResource::kInodes, 1}}));
	EXPECT_FALSE(database.exceeds(QuotaOwnerType::kUser, 999, QuotaRigor::kHard, {{QuotaResource::kInodes, 1}}));
	EXPECT_TRUE(database.exceeds(QuotaOwnerType::kGroup, 999, QuotaRigor::kHard, {{QuotaResource::kInodes, 1}}));
	EXPECT_FALSE(database.exceeds(QuotaOwnerType::kUser, 999, QuotaRigor::kSoft, {{QuotaResource::kSize, 1}}));
	EXPECT_FALSE(database.exceeds(QuotaOwnerType::kGroup, 999, QuotaRigor::kSoft, {{QuotaResource::kSize, 1}}));
	EXPECT_FALSE(database.exceeds(QuotaOwnerType::kUser, 999, QuotaRigor::kHard, {{QuotaResource::kSize, 1}}));
	EXPECT_FALSE(database.exceeds(QuotaOwnerType::kGroup, 999, QuotaRigor::kHard, {{QuotaResource::kSize, 1}}));
}

TEST(QuotaDatabaseTests, IsExceededCornerCase) {
	QuotaDatabase database;

	database.set(QuotaOwnerType::kUser, 888, QuotaRigor::kSoft, QuotaResource::kInodes, 100);
	database.set(QuotaOwnerType::kUser, 888, QuotaRigor::kHard, QuotaResource::kInodes, 200);

	database.update(QuotaOwnerType::kUser, 888, QuotaRigor::kUsed, QuotaResource::kInodes, 100);
	database.update(QuotaOwnerType::kGroup, 888, QuotaRigor::kUsed, QuotaResource::kInodes, 100);
	EXPECT_FALSE(database.exceeds(QuotaOwnerType::kUser, 888, QuotaRigor::kSoft, {{QuotaResource::kInodes, 0}}));
	EXPECT_FALSE(database.exceeds(QuotaOwnerType::kGroup, 888, QuotaRigor::kSoft, {{QuotaResource::kInodes, 101}}));
	EXPECT_FALSE(database.exceeds(QuotaOwnerType::kUser, 888, QuotaRigor::kHard, {{QuotaResource::kInodes, 100}}));
	EXPECT_FALSE(database.exceeds(QuotaOwnerType::kGroup, 888, QuotaRigor::kHard, {{QuotaResource::kInodes, 101}}));
	EXPECT_TRUE(database.exceeds(QuotaOwnerType::kUser, 888, QuotaRigor::kSoft, {{QuotaResource::kInodes, 1}}));
	EXPECT_TRUE(database.exceeds(QuotaOwnerType::kUser, 888, QuotaRigor::kHard, {{QuotaResource::kInodes, 101}}));
}

TEST(QuotaDatabaseTests, Checksum) {
	QuotaDatabase database;
	std::vector<uint64_t> checksum(8);
	checksum[0] = database.checksum();
	// Collect checksums with single param change
	database.set(QuotaOwnerType::kUser, 998, QuotaRigor::kSoft, QuotaResource::kInodes, 200);
	checksum[1] = database.checksum();
	database.set(QuotaOwnerType::kUser, 998, QuotaRigor::kSoft, QuotaResource::kInodes, 300);
	checksum[2] = database.checksum();
	database.set(QuotaOwnerType::kUser, 999, QuotaRigor::kSoft, QuotaResource::kInodes, 200);
	checksum[3] = database.checksum();
	database.set(QuotaOwnerType::kGroup, 998, QuotaRigor::kSoft, QuotaResource::kInodes, 200);
	checksum[4] = database.checksum();
	database.set(QuotaOwnerType::kUser, 998, QuotaRigor::kSoft, QuotaResource::kSize, 200);
	checksum[5] = database.checksum();
	database.set(QuotaOwnerType::kUser, 998, QuotaRigor::kHard, QuotaResource::kInodes, 200);
	checksum[6] = database.checksum();
	// Adding another limit changes a checksum
	database.set(QuotaOwnerType::kGroup, 998, QuotaRigor::kHard, QuotaResource::kInodes, 200);
	checksum[7] = database.checksum();
	for (int i = 0; i < 8; ++i) {
		for (int j = 0; j < 8; ++j) {
			if (i == j) {
				continue;
			}
			EXPECT_NE(checksum[i], checksum[j]);
		}
	}

	// No change use cases:
	// Rewrite the same limit
	database.set(QuotaOwnerType::kGroup, 998, QuotaRigor::kHard, QuotaResource::kInodes, 200);
	EXPECT_EQ(checksum[7], database.checksum());

	// Set usage
	database.update(QuotaOwnerType::kUser, 998, QuotaRigor::kUsed, QuotaResource::kInodes, 500);
	database.update(QuotaOwnerType::kGroup, 998, QuotaRigor::kUsed, QuotaResource::kInodes, 500);
	EXPECT_EQ(checksum[7], database.checksum());
}
