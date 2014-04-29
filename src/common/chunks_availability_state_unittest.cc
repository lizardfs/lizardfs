#include "config.h"
#include "common/chunks_availability_state.h"

#include <gtest/gtest.h>

static uint8_t xor2 = xorLevelToGoal(2);
static uint8_t xor3 = xorLevelToGoal(3);

TEST(ChunksAvailabilityStateTests, AddRemoveChunk) {
	ChunksAvailabilityState s;
	s.addChunk(0, ChunksAvailabilityState::kSafe);
	s.addChunk(0, ChunksAvailabilityState::kSafe);
	s.addChunk(2, ChunksAvailabilityState::kSafe);
	s.addChunk(xor2, ChunksAvailabilityState::kSafe);
	s.addChunk(xor2, ChunksAvailabilityState::kEndangered);
	s.addChunk(xor2, ChunksAvailabilityState::kEndangered);
	s.addChunk(xor3, ChunksAvailabilityState::kEndangered);
	s.addChunk(xor2, ChunksAvailabilityState::kLost);

	EXPECT_EQ(2U, s.safeChunks(0));
	EXPECT_EQ(0U, s.endangeredChunks(0));
	EXPECT_EQ(0U, s.lostChunks(0));

	EXPECT_EQ(1U, s.safeChunks(2));
	EXPECT_EQ(0U, s.endangeredChunks(2));
	EXPECT_EQ(0U, s.lostChunks(2));

	EXPECT_EQ(1U, s.safeChunks(xor2));
	EXPECT_EQ(2U, s.endangeredChunks(xor2));
	EXPECT_EQ(1U, s.lostChunks(xor2));

	EXPECT_EQ(0U, s.safeChunks(xor3));
	EXPECT_EQ(1U, s.endangeredChunks(xor3));
	EXPECT_EQ(0U, s.lostChunks(xor3));

	// Make endangered chunks safe
	s.removeChunk(xor2, ChunksAvailabilityState::kEndangered);
	s.addChunk(xor2, ChunksAvailabilityState::kSafe);
	s.removeChunk(xor2, ChunksAvailabilityState::kEndangered);
	s.addChunk(xor2, ChunksAvailabilityState::kSafe);
	s.removeChunk(xor3, ChunksAvailabilityState::kEndangered);
	s.addChunk(xor3, ChunksAvailabilityState::kSafe);

	EXPECT_EQ(2U, s.safeChunks(0));
	EXPECT_EQ(0U, s.endangeredChunks(0));
	EXPECT_EQ(0U, s.lostChunks(0));

	EXPECT_EQ(1U, s.safeChunks(2));
	EXPECT_EQ(0U, s.endangeredChunks(2));
	EXPECT_EQ(0U, s.lostChunks(2));

	EXPECT_EQ(3U, s.safeChunks(xor2));
	EXPECT_EQ(0U, s.endangeredChunks(xor2));
	EXPECT_EQ(1U, s.lostChunks(xor2));

	EXPECT_EQ(1U, s.safeChunks(xor3));
	EXPECT_EQ(0U, s.endangeredChunks(xor3));
	EXPECT_EQ(0U, s.lostChunks(xor3));

	// Remove some safe chunks
	s.removeChunk(0, ChunksAvailabilityState::kSafe);
	s.removeChunk(0, ChunksAvailabilityState::kSafe);
	s.removeChunk(2, ChunksAvailabilityState::kSafe);

	EXPECT_EQ(0U, s.safeChunks(0));
	EXPECT_EQ(0U, s.endangeredChunks(0));
	EXPECT_EQ(0U, s.lostChunks(0));

	EXPECT_EQ(0U, s.safeChunks(2));
	EXPECT_EQ(0U, s.endangeredChunks(2));
	EXPECT_EQ(0U, s.lostChunks(2));
}

TEST(ChunksReplicationStateTests, AddRemoveChunk) {
	ChunksReplicationState s;
	s.addChunk(0, 0, 8);
	s.addChunk(2, 0, 0);
	s.addChunk(2, 1, 0);
	s.addChunk(xor2, 0, 4);
	s.addChunk(xor2, 1, 4);

	EXPECT_EQ(1U, s.chunksToReplicate(0, 0));
	EXPECT_EQ(1U, s.chunksToDelete(0, 8));
	EXPECT_EQ(1U, s.chunksToReplicate(2, 0));
	EXPECT_EQ(1U, s.chunksToReplicate(2, 1));
	EXPECT_EQ(2U, s.chunksToDelete(2, 0));
	EXPECT_EQ(1U, s.chunksToReplicate(xor2, 1));
	EXPECT_EQ(1U, s.chunksToReplicate(xor2, 0));
	EXPECT_EQ(2U, s.chunksToDelete(xor2, 4));

	// Replicate missing chunks
	s.removeChunk(2, 1, 0);
	s.addChunk(2, 0, 0);
	s.removeChunk(xor2, 1, 4);
	s.addChunk(xor2, 0, 4);

	EXPECT_EQ(2U, s.chunksToReplicate(2, 0));
	EXPECT_EQ(0U, s.chunksToReplicate(2, 1));
	EXPECT_EQ(2U, s.chunksToDelete(2, 0));
	EXPECT_EQ(0U, s.chunksToReplicate(xor2, 1));
	EXPECT_EQ(2U, s.chunksToReplicate(xor2, 0));
	EXPECT_EQ(2U, s.chunksToDelete(xor2, 4));
}

TEST(ChunksReplicationStateTests, MaximumValues) {
	ChunksReplicationState s;
	s.addChunk(xor2, 1500, 2000);
	s.addChunk(xor2, 1501, 2001);
	EXPECT_EQ(2U, s.chunksToReplicate(xor2, ChunksReplicationState::kMaxPartsCount));
	EXPECT_EQ(2U, s.chunksToDelete(xor2, ChunksReplicationState::kMaxPartsCount));
}
