#include "config.h"
#include "master/quota_database.h"

#include <gtest/gtest.h>

#define EXPECT_ENTRY_EQ(entry, ivalue, svalue, isoft, ihard, ssoft, shard) \
		ASSERT_NE(nullptr, entry); \
		EXPECT_EQ(ivalue, entry->inodes); \
		EXPECT_EQ(svalue, entry->bytes); \
		EXPECT_EQ(isoft, entry->inodesSoftLimit); \
		EXPECT_EQ(ihard, entry->inodesHardLimit); \
		EXPECT_EQ(ssoft, entry->bytesSoftLimit); \
		EXPECT_EQ(shard, entry->bytesHardLimit);

TEST(QuotaDatabaseTests, SetGetQuota) {
	QuotaDatabase database;
	ASSERT_EQ(nullptr, database.get(QuotaOwnerType::kUser, 999));

	database.set(QuotaRigor::kSoft, QuotaResource::kInodes, QuotaOwnerType::kUser, 999, 100);
	ASSERT_EQ(nullptr, database.get(QuotaOwnerType::kGroup, 999));
	EXPECT_ENTRY_EQ(database.get(QuotaOwnerType::kUser, 999), 0U, 0U, 100U, 0U, 0U, 0U);

	database.set(QuotaRigor::kHard, QuotaResource::kInodes, QuotaOwnerType::kUser, 999, 200);
	ASSERT_EQ(nullptr, database.get(QuotaOwnerType::kGroup, 999));
	EXPECT_ENTRY_EQ(database.get(QuotaOwnerType::kUser, 999), 0U, 0U, 100U, 200U, 0U, 0U);

	database.set(QuotaRigor::kSoft, QuotaResource::kSize, QuotaOwnerType::kUser, 999, 300);
	ASSERT_EQ(nullptr, database.get(QuotaOwnerType::kGroup, 999));
	EXPECT_ENTRY_EQ(database.get(QuotaOwnerType::kUser, 999), 0U, 0U, 100U, 200U, 300U, 0U);

	database.set(QuotaRigor::kHard, QuotaResource::kSize, QuotaOwnerType::kUser, 999, 400);
	ASSERT_EQ(nullptr, database.get(QuotaOwnerType::kGroup, 999));
	EXPECT_ENTRY_EQ(database.get(QuotaOwnerType::kUser, 999), 0U, 0U, 100U, 200U, 300U, 400U);
}

TEST(QuotaDatabaseTests, RemoveQuota) {
	QuotaDatabase database;
	database.set(QuotaRigor::kSoft, QuotaResource::kInodes, QuotaOwnerType::kGroup, 999, 100);
	database.set(QuotaRigor::kHard, QuotaResource::kInodes, QuotaOwnerType::kGroup, 999, 200);
	EXPECT_ENTRY_EQ(database.get(QuotaOwnerType::kGroup, 999), 0U, 0U, 100U, 200U, 0U, 0U);

	database.remove(QuotaRigor::kSoft, QuotaResource::kInodes, QuotaOwnerType::kGroup, 999);
	EXPECT_ENTRY_EQ(database.get(QuotaOwnerType::kGroup, 999), 0U, 0U, 0U, 200U, 0U, 0U);

	database.remove(QuotaRigor::kHard, QuotaResource::kInodes, QuotaOwnerType::kGroup, 999);
	EXPECT_ENTRY_EQ(database.get(QuotaOwnerType::kGroup, 999), 0U, 0U, 0U, 0U, 0U, 0U);
}

TEST(QuotaDatabaseTests, IsExceeded) {
	QuotaDatabase database;
	database.set(QuotaRigor::kSoft, QuotaResource::kInodes, QuotaOwnerType::kUser, 999, 100);
	database.set(QuotaRigor::kHard, QuotaResource::kInodes, QuotaOwnerType::kUser, 999, 200);
	database.set(QuotaRigor::kSoft, QuotaResource::kInodes, QuotaOwnerType::kGroup, 999, 300);
	database.set(QuotaRigor::kHard, QuotaResource::kInodes, QuotaOwnerType::kGroup, 999, 400);
	database.set(QuotaRigor::kSoft, QuotaResource::kSize, QuotaOwnerType::kUser, 999, 1000);
	database.set(QuotaRigor::kHard, QuotaResource::kSize, QuotaOwnerType::kUser, 999, 2000);
	database.set(QuotaRigor::kSoft, QuotaResource::kSize, QuotaOwnerType::kGroup, 999, 3000);
	database.set(QuotaRigor::kHard, QuotaResource::kSize, QuotaOwnerType::kGroup, 999, 4000);

	database.changeUsage(QuotaResource::kInodes, 999, 999, 150);
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kSoft, QuotaResource::kInodes, 999, 999));
	EXPECT_FALSE(database.isExceeded(QuotaRigor::kHard, QuotaResource::kInodes, 999, 999));
	EXPECT_FALSE(database.isExceeded(QuotaRigor::kSoft, QuotaResource::kSize, 999, 999));
	EXPECT_FALSE(database.isExceeded(QuotaRigor::kHard, QuotaResource::kSize, 999, 999));

	database.changeUsage(QuotaResource::kInodes, 1, 999, 270);
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kSoft, QuotaResource::kInodes, 999, 999));
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kHard, QuotaResource::kInodes, 999, 999));
	EXPECT_FALSE(database.isExceeded(QuotaRigor::kSoft, QuotaResource::kSize, 999, 999));
	EXPECT_FALSE(database.isExceeded(QuotaRigor::kHard, QuotaResource::kSize, 999, 999));

	database.changeUsage(QuotaResource::kSize, 1, 999, 2500);
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kSoft, QuotaResource::kInodes, 999, 999));
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kHard, QuotaResource::kInodes, 999, 999));
	EXPECT_FALSE(database.isExceeded(QuotaRigor::kSoft, QuotaResource::kSize, 999, 999));
	EXPECT_FALSE(database.isExceeded(QuotaRigor::kHard, QuotaResource::kSize, 999, 999));

	database.changeUsage(QuotaResource::kSize, 2, 999, 1000);
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kSoft, QuotaResource::kInodes, 999, 999));
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kHard, QuotaResource::kInodes, 999, 999));
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kSoft, QuotaResource::kSize, 999, 999));
	EXPECT_FALSE(database.isExceeded(QuotaRigor::kHard, QuotaResource::kSize, 999, 999));

	database.changeUsage(QuotaResource::kSize, 999, 7, 10000);
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kSoft, QuotaResource::kInodes, 999, 999));
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kHard, QuotaResource::kInodes, 999, 999));
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kSoft, QuotaResource::kSize, 999, 999));
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kHard, QuotaResource::kSize, 999, 999));

	database.changeUsage(QuotaResource::kSize, 999, 7, -10000);
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kSoft, QuotaResource::kInodes, 999, 999));
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kHard, QuotaResource::kInodes, 999, 999));
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kSoft, QuotaResource::kSize, 999, 999));
	EXPECT_FALSE(database.isExceeded(QuotaRigor::kHard, QuotaResource::kSize, 999, 999));

	database.set(QuotaRigor::kSoft, QuotaResource::kSize, QuotaOwnerType::kGroup, 999, 3700);
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kSoft, QuotaResource::kInodes, 999, 999));
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kHard, QuotaResource::kInodes, 999, 999));
	EXPECT_FALSE(database.isExceeded(QuotaRigor::kSoft, QuotaResource::kSize, 999, 999));
	EXPECT_FALSE(database.isExceeded(QuotaRigor::kHard, QuotaResource::kSize, 999, 999));

	database.remove(QuotaRigor::kHard, QuotaResource::kInodes, QuotaOwnerType::kGroup, 999);
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kSoft, QuotaResource::kInodes, 999, 999));
	EXPECT_FALSE(database.isExceeded(QuotaRigor::kHard, QuotaResource::kInodes, 999, 999));
	EXPECT_FALSE(database.isExceeded(QuotaRigor::kSoft, QuotaResource::kSize, 999, 999));
	EXPECT_FALSE(database.isExceeded(QuotaRigor::kHard, QuotaResource::kSize, 999, 999));
}

TEST(QuotaDatabaseTests, IsExceededCornerCase) {
	QuotaDatabase database;
	database.set(QuotaRigor::kSoft, QuotaResource::kInodes, QuotaOwnerType::kUser, 888, 100);
	database.set(QuotaRigor::kHard, QuotaResource::kInodes, QuotaOwnerType::kUser, 888, 200);

	database.changeUsage(QuotaResource::kInodes, 888, 888, 100);
	EXPECT_FALSE(database.isExceeded(QuotaRigor::kSoft, QuotaResource::kInodes, 888, 888));
	EXPECT_FALSE(database.isExceeded(QuotaRigor::kHard, QuotaResource::kInodes, 888, 888));
	database.changeUsage(QuotaResource::kInodes, 888, 888, 1); // 101 now
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kSoft, QuotaResource::kInodes, 888, 888));
	EXPECT_FALSE(database.isExceeded(QuotaRigor::kHard, QuotaResource::kInodes, 888, 888));
	database.changeUsage(QuotaResource::kInodes, 888, 888, 98); // 199 now
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kSoft, QuotaResource::kInodes, 888, 888));
	EXPECT_FALSE(database.isExceeded(QuotaRigor::kHard, QuotaResource::kInodes, 888, 888));
	database.changeUsage(QuotaResource::kInodes, 888, 888, 1); // 200 now
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kSoft, QuotaResource::kInodes, 888, 888));
	EXPECT_TRUE(database.isExceeded(QuotaRigor::kHard, QuotaResource::kInodes, 888, 888));
}
