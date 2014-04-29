#include "config.h"
#include "master/io_limits_database.h"

#include <gtest/gtest.h>

static const IoLimitsDatabase::ClientId c1 = IoLimitsDatabase::ClientId(1111);
static const IoLimitsDatabase::ClientId c2 = IoLimitsDatabase::ClientId(2222);

static IoLimitsDatabase makeDatabase(const std::vector<IoLimitsDatabase::ClientId>& clients,
		const IoLimitsConfigLoader::LimitsMap& limits) {
	IoLimitsDatabase database;
	for (auto client : clients) {
		database.addClient(client);
	}
	database.setLimits(limits);
	return database;
}

TEST(IoLimitsDatabaseTests, DefaultZero) {
	IoLimitsDatabase database = makeDatabase({c1}, {{"groupa", 1}});
	EXPECT_EQ(0U, database.getAllocation(c1, "groupa"));
}

TEST(IoLimitsDatabaseTests, ChangeLimit) {
	IoLimitsDatabase database = makeDatabase({c1}, {{"groupa", 4}});
	EXPECT_EQ(1024U, database.setAllocation(c1, "groupa", 1024));
	EXPECT_EQ(2048U, database.setAllocation(c1, "groupa", 2048));
	EXPECT_EQ(4096U, database.setAllocation(c1, "groupa", 8192));
	EXPECT_EQ(4096U, database.getAllocation(c1, "groupa"));
}

TEST(IoLimitsDatabaseTests, TwoClients) {
	IoLimitsDatabase database = makeDatabase({c1, c2}, {{"groupa", 3}});
	EXPECT_EQ(1024U, database.setAllocation(c1, "groupa", 1024));
	EXPECT_EQ(2048U, database.setAllocation(c2, "groupa", 3072));
}

TEST(IoLimitsDatabaseTests, TwoGroups) {
	IoLimitsDatabase database = makeDatabase({c1}, {{"groupa", 4}, {"groupb", 8}});
	EXPECT_EQ(4096U, database.setAllocation(c1, "groupa", 4096));
	EXPECT_EQ(8192U, database.setAllocation(c1, "groupb", 8192));
}

TEST(IoLimitsDatabaseTests, ReduceAllocation) {
	IoLimitsDatabase database = makeDatabase({c1, c2}, {{"groupa", 8}});
	EXPECT_EQ(4096U, database.setAllocation(c1, "groupa", 4096));
	EXPECT_EQ(4096U, database.setAllocation(c2, "groupa", 6144));
	EXPECT_EQ(2048U, database.setAllocation(c1, "groupa", 2048));
	EXPECT_EQ(6144U, database.setAllocation(c2, "groupa", 6144));
}

TEST(IoLimitsDatabaseTests, RemoveClient) {
	IoLimitsDatabase database = makeDatabase({c1, c2}, {{"groupa", 8}});
	EXPECT_EQ(4096U, database.setAllocation(c1, "groupa", 4096));
	EXPECT_EQ(4096U, database.setAllocation(c2, "groupa", 8192));
	database.removeClient(c1);
	EXPECT_EQ(8192U, database.setAllocation(c2, "groupa", 8192));
}

TEST(IoLimitsDatabaseTests, ThrowInvalidClient) {
	IoLimitsDatabase database = makeDatabase({}, {{"groupa", 1}});
	EXPECT_THROW(database.getAllocation(c1, "groupa"),
			IoLimitsDatabase::InvalidClientIdException);
}

TEST(IoLimitsDatabaseTests, ThrowInvalidGroup) {
	IoLimitsDatabase database = makeDatabase({c1}, {});
	EXPECT_THROW(database.getAllocation(c1, "groupa"),
			IoLimitsDatabase::InvalidGroupIdException);
}

TEST(IoLimitsDatabaseTests, ThrowClientExists) {
	IoLimitsDatabase database = makeDatabase({c1}, {});
	EXPECT_THROW(database.addClient(c1), IoLimitsDatabase::ClientExistsException);
}

TEST(IoLimitsDatabaseTests, ChangeConfig) {
	IoLimitsDatabase database = makeDatabase({c1}, {});
	IoLimitsConfigLoader::LimitsMap config;

	config["groupa"] = 32;
	database.setLimits(config);

	EXPECT_EQ(32768U, database.setAllocation(c1, "groupa", 32768));

	config.erase("groupa");
	config["groupb"] = 64;

	EXPECT_NO_THROW(database.getAllocation(c1, "groupa"));
	EXPECT_THROW(database.getAllocation(c1, "groupb"),
			IoLimitsDatabase::InvalidGroupIdException);

	database.setLimits(config);

	EXPECT_THROW(database.getAllocation(c1, "groupa"),
			IoLimitsDatabase::InvalidGroupIdException);
	EXPECT_NO_THROW(database.getAllocation(c1, "groupb"));
	EXPECT_EQ(65536U, database.setAllocation(c1, "groupb", 65536));
}
