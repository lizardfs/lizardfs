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
#include "common/chunks_availability_state.h"

#include <gtest/gtest.h>

TEST(ChunksAvailabilityStateTests, AddRemoveChunk) {
	ChunksAvailabilityState s;

	s.addChunk(0, ChunksAvailabilityState::kSafe);
	s.addChunk(0, ChunksAvailabilityState::kSafe);
	s.addChunk(2, ChunksAvailabilityState::kSafe);
	s.addChunk(10, ChunksAvailabilityState::kSafe);
	s.addChunk(10, ChunksAvailabilityState::kEndangered);
	s.addChunk(10, ChunksAvailabilityState::kEndangered);
	s.addChunk(11, ChunksAvailabilityState::kEndangered);
	s.addChunk(10, ChunksAvailabilityState::kLost);

	EXPECT_EQ(2U, s.safeChunks(0));
	EXPECT_EQ(0U, s.endangeredChunks(0));
	EXPECT_EQ(0U, s.lostChunks(0));

	EXPECT_EQ(1U, s.safeChunks(2));
	EXPECT_EQ(0U, s.endangeredChunks(2));
	EXPECT_EQ(0U, s.lostChunks(2));

	EXPECT_EQ(1U, s.safeChunks(10));
	EXPECT_EQ(2U, s.endangeredChunks(10));
	EXPECT_EQ(1U, s.lostChunks(10));

	EXPECT_EQ(0U, s.safeChunks(11));
	EXPECT_EQ(1U, s.endangeredChunks(11));
	EXPECT_EQ(0U, s.lostChunks(11));

	// Make endangered chunks safe
	s.removeChunk(10, ChunksAvailabilityState::kEndangered);
	s.addChunk(10, ChunksAvailabilityState::kSafe);
	s.removeChunk(10, ChunksAvailabilityState::kEndangered);
	s.addChunk(10, ChunksAvailabilityState::kSafe);
	s.removeChunk(11, ChunksAvailabilityState::kEndangered);
	s.addChunk(11, ChunksAvailabilityState::kSafe);

	EXPECT_EQ(2U, s.safeChunks(0));
	EXPECT_EQ(0U, s.endangeredChunks(0));
	EXPECT_EQ(0U, s.lostChunks(0));

	EXPECT_EQ(1U, s.safeChunks(2));
	EXPECT_EQ(0U, s.endangeredChunks(2));
	EXPECT_EQ(0U, s.lostChunks(2));

	EXPECT_EQ(3U, s.safeChunks(10));
	EXPECT_EQ(0U, s.endangeredChunks(10));
	EXPECT_EQ(1U, s.lostChunks(10));

	EXPECT_EQ(1U, s.safeChunks(11));
	EXPECT_EQ(0U, s.endangeredChunks(11));
	EXPECT_EQ(0U, s.lostChunks(11));

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
	s.addChunk(10, 0, 4);
	s.addChunk(10, 1, 4);

	EXPECT_EQ(1U, s.chunksToReplicate(0, 0));
	EXPECT_EQ(1U, s.chunksToDelete(0, 8));
	EXPECT_EQ(1U, s.chunksToReplicate(2, 0));
	EXPECT_EQ(1U, s.chunksToReplicate(2, 1));
	EXPECT_EQ(2U, s.chunksToDelete(2, 0));
	EXPECT_EQ(1U, s.chunksToReplicate(10, 1));
	EXPECT_EQ(1U, s.chunksToReplicate(10, 0));
	EXPECT_EQ(2U, s.chunksToDelete(10, 4));

	// Replicate missing chunks
	s.removeChunk(2, 1, 0);
	s.addChunk(2, 0, 0);
	s.removeChunk(10, 1, 4);
	s.addChunk(10, 0, 4);

	EXPECT_EQ(2U, s.chunksToReplicate(2, 0));
	EXPECT_EQ(0U, s.chunksToReplicate(2, 1));
	EXPECT_EQ(2U, s.chunksToDelete(2, 0));
	EXPECT_EQ(0U, s.chunksToReplicate(10, 1));
	EXPECT_EQ(2U, s.chunksToReplicate(10, 0));
	EXPECT_EQ(2U, s.chunksToDelete(10, 4));
}

TEST(ChunksReplicationStateTests, MaximumValues) {
	ChunksReplicationState s;
	s.addChunk(10, 1500, 2000);
	s.addChunk(10, 1501, 2001);
	EXPECT_EQ(2U, s.chunksToReplicate(10, ChunksReplicationState::kMaxPartsCount - 1));
	EXPECT_EQ(2U, s.chunksToDelete(10, ChunksReplicationState::kMaxPartsCount - 1));
}
