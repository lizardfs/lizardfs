#include "mfsmount/chunkserver_stats.h"

#include <gtest/gtest.h>

TEST(ChunkserverStatsTests, ChunkserverStatsCounters) {
	ChunkserverStats stats;
	NetworkAddress server1(1111, 11);
	NetworkAddress server2(2222, 22);

	EXPECT_EQ(0u, stats.getStatisticsFor(server1).pendingReads());
	EXPECT_EQ(0u, stats.getStatisticsFor(server2).pendingReads());
	EXPECT_EQ(0u, stats.getStatisticsFor(server1).pendingWrites());
	EXPECT_EQ(0u, stats.getStatisticsFor(server2).pendingWrites());

	stats.registerReadOperation(server1);
	EXPECT_EQ(1u, stats.getStatisticsFor(server1).pendingReads());

	stats.registerReadOperation(server2);
	EXPECT_EQ(1u, stats.getStatisticsFor(server1).pendingReads());
	EXPECT_EQ(1u, stats.getStatisticsFor(server2).pendingReads());

	EXPECT_EQ(1u, stats.getStatisticsFor(server1).getOperationCount());

	stats.unregisterReadOperation(server1);
	EXPECT_EQ(0u, stats.getStatisticsFor(server1).pendingReads());
	EXPECT_EQ(1u, stats.getStatisticsFor(server2).pendingReads());

	stats.registerReadOperation(server1);
	stats.registerReadOperation(server1);
	EXPECT_EQ(2u, stats.getStatisticsFor(server1).pendingReads());
	EXPECT_EQ(0u, stats.getStatisticsFor(server1).pendingWrites());

	stats.registerWriteOperation(server1);
	stats.registerWriteOperation(server1);
	stats.registerWriteOperation(server1);
	EXPECT_EQ(2u, stats.getStatisticsFor(server1).pendingReads());
	EXPECT_EQ(3u, stats.getStatisticsFor(server1).pendingWrites());
	EXPECT_EQ(5u, stats.getStatisticsFor(server1).getOperationCount());

	stats.unregisterWriteOperation(server1);
	stats.unregisterWriteOperation(server1);
	EXPECT_EQ(2u, stats.getStatisticsFor(server1).pendingReads());
	EXPECT_EQ(1u, stats.getStatisticsFor(server1).pendingWrites());
	EXPECT_EQ(3u, stats.getStatisticsFor(server1).getOperationCount());

	stats.unregisterReadOperation(server2);
	EXPECT_EQ(0u, stats.getStatisticsFor(server2).getOperationCount());
}

TEST(ChunkserverStatsTests, ChunkserverStatsDefectTracking) {
	ChunkserverStats stats;
	NetworkAddress server1(1111, 11);
	EXPECT_FALSE(stats.getStatisticsFor(server1).isDefective());
	stats.markDefective(server1);
	EXPECT_TRUE(stats.getStatisticsFor(server1).isDefective());
	stats.markWorking(server1);
	EXPECT_FALSE(stats.getStatisticsFor(server1).isDefective());
}

TEST(ChunkserverStatsTests, ChunkserverStatsProxy) {
	ChunkserverStats stats;
	NetworkAddress server1(1111, 11);

	{
		ChunkserverStatsProxy proxy(stats);

		proxy.registerReadOperation(server1);
		proxy.registerReadOperation(server1);
		EXPECT_EQ(2u, stats.getStatisticsFor(server1).pendingReads());

		proxy.unregisterReadOperation(server1);
		EXPECT_EQ(1u, stats.getStatisticsFor(server1).pendingReads());

		proxy.registerWriteOperation(server1);
		proxy.registerWriteOperation(server1);
		EXPECT_EQ(2u, stats.getStatisticsFor(server1).pendingWrites());

		proxy.unregisterWriteOperation(server1);
		EXPECT_EQ(1u, stats.getStatisticsFor(server1).pendingWrites());
	}
	EXPECT_EQ(0u, stats.getStatisticsFor(server1).pendingReads());
	EXPECT_EQ(0u, stats.getStatisticsFor(server1).pendingWrites());
}

TEST(ChunkserverStatsTests, AllPendingDefectiveRead) {
	ChunkserverStats stats;
	NetworkAddress server1(1111, 11);
	ChunkserverStatsProxy proxy(stats);

	proxy.registerReadOperation(server1);
	EXPECT_FALSE(stats.getStatisticsFor(server1).isDefective());
	proxy.allPendingDefective();
	EXPECT_TRUE(stats.getStatisticsFor(server1).isDefective());
}

TEST(ChunkserverStatsTests, AllPendingDefectiveWrite) {
	ChunkserverStats stats;
	NetworkAddress server1(1111, 11);
	ChunkserverStatsProxy proxy(stats);

	proxy.registerWriteOperation(server1);
	EXPECT_FALSE(stats.getStatisticsFor(server1).isDefective());
	proxy.allPendingDefective();
	EXPECT_TRUE(stats.getStatisticsFor(server1).isDefective());
}

TEST(ChunkserverStatsTests, AllPendingDefectiveLonger) {
	ChunkserverStats stats;
	NetworkAddress server1(1111, 11);
	NetworkAddress server2(2222, 22);
	ChunkserverStatsProxy proxy(stats);

	proxy.registerReadOperation(server1);
	proxy.registerReadOperation(server2);
	proxy.registerReadOperation(server2);
	proxy.unregisterReadOperation(server1);
	proxy.unregisterReadOperation(server2);
	proxy.allPendingDefective();
	EXPECT_FALSE(stats.getStatisticsFor(server1).isDefective());
	EXPECT_TRUE(stats.getStatisticsFor(server2).isDefective());
}
