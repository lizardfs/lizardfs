#include "master/io_limits_database.h"

#include <gtest/gtest.h>

static const IoLimitsDatabase::ClientId c1 = 1111;
static const IoLimitsDatabase::ClientId c2 = 2222;

TEST(IoLimitsDatabaseTests, DefaultZero) {
	IoLimitsDatabase database;
	IoLimitsConfigLoader::LimitsMap config;
	config["carrot"] = 1234;
	database.setLimits(config);
	database.addClient(c1);
	EXPECT_EQ(0U, database.getAllocation(c1, "carrot"));
}

TEST(IoLimitsDatabaseTests, ChangeLimit) {
	IoLimitsDatabase database;
	IoLimitsConfigLoader::LimitsMap config;
	config["carrot"] = 1234;
	database.setLimits(config);
	database.addClient(c1);
	EXPECT_EQ(123U, database.setAllocation(c1, "carrot", 123));
	EXPECT_EQ(234U, database.setAllocation(c1, "carrot", 234));
	EXPECT_EQ(789U, database.setAllocation(c1, "carrot", 789));
	EXPECT_EQ(1234U, database.setAllocation(c1, "carrot", 123456789));
	EXPECT_EQ(1234U, database.getAllocation(c1, "carrot"));
}

TEST(IoLimitsDatabaseTests, TwoClients) {
	IoLimitsDatabase database;
	IoLimitsConfigLoader::LimitsMap config;
	config["carrot"] = 1234;
	database.setLimits(config);
	database.addClient(c1);
	database.addClient(c2);
	EXPECT_EQ(789U, database.setAllocation(c1, "carrot", 789));
	EXPECT_EQ(1234U-789U, database.setAllocation(c2, "carrot", 789));
}

TEST(IoLimitsDatabaseTests, TwoGroups) {
	IoLimitsDatabase database;
	IoLimitsConfigLoader::LimitsMap config;
	config["carrot"] = 1234;
	config["pea"] = 2345;
	database.setLimits(config);
	database.addClient(c1);

	EXPECT_EQ(1234U, database.setAllocation(c1, "carrot", 12345678));
	EXPECT_EQ(2345U, database.setAllocation(c1, "pea", 23456789));
}

TEST(IoLimitsDatabaseTests, ReduceAllocation) {
	IoLimitsDatabase database;
	IoLimitsConfigLoader::LimitsMap config;
	config["carrot"] = 1234;
	database.setLimits(config);
	database.addClient(c1);
	database.addClient(c2);
	EXPECT_EQ(789U, database.setAllocation(c1, "carrot", 789));
	EXPECT_EQ(1234U-789U, database.setAllocation(c2, "carrot", 789));
	EXPECT_EQ(123U, database.setAllocation(c1, "carrot", 123));
	EXPECT_EQ(789U, database.setAllocation(c2, "carrot", 789));
}

TEST(IoLimitsDatabaseTests, RemoveClient) {
	IoLimitsDatabase database;
	IoLimitsConfigLoader::LimitsMap config;
	config["carrot"] = 1234;
	database.setLimits(config);
	database.addClient(c1);
	database.addClient(c2);
	EXPECT_EQ(789U, database.setAllocation(c1, "carrot", 789));
	EXPECT_EQ(1234U-789U, database.setAllocation(c2, "carrot", 789));
	database.removeClient(c1);
	EXPECT_EQ(789U, database.setAllocation(c2, "carrot", 789));
}

TEST(IoLimitsDatabaseTests, ThrowInvalidClient) {
	IoLimitsDatabase database;
	IoLimitsConfigLoader::LimitsMap config;
	config["carrot"] = 1234;
	database.setLimits(config);
	EXPECT_THROW(database.getAllocation(c1, "carrot"),
			IoLimitsDatabase::InvalidClientIdException);
}

TEST(IoLimitsDatabaseTests, ThrowInvalidGroup) {
	IoLimitsDatabase database;
	IoLimitsConfigLoader::LimitsMap config;
	database.setLimits(config);
	database.addClient(c1);
	EXPECT_THROW(database.getAllocation(c1, "carrot"),
			IoLimitsDatabase::InvalidGroupIdException);
}

TEST(IoLimitsDatabaseTests, ThrowClientExists) {
	IoLimitsDatabase database;
	database.addClient(c1);
	EXPECT_THROW(database.addClient(c1), IoLimitsDatabase::ClientExistsException);
}

TEST(IoLimitsDatabaseTests, ChangeConfig) {
	IoLimitsDatabase database;
	IoLimitsConfigLoader::LimitsMap config;
	config["carrot"] = 1234;
	database.setLimits(config);
	database.addClient(c1);
	EXPECT_EQ(1234U, database.setAllocation(c1, "carrot", 1234));

	config.erase("carrot");
	config["pea"] = 1234;

	EXPECT_NO_THROW(database.getAllocation(c1, "carrot"));
	EXPECT_THROW(database.getAllocation(c1, "pea"),
			IoLimitsDatabase::InvalidGroupIdException);

	database.setLimits(config);

	EXPECT_THROW(database.getAllocation(c1, "carrot"),
			IoLimitsDatabase::InvalidGroupIdException);
	EXPECT_NO_THROW(database.getAllocation(c1, "pea"));

	EXPECT_EQ(1234U, database.setAllocation(c1, "pea", 1234));
}
