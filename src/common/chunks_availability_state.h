#pragma once

#include "common/chunk_type.h"
#include "common/goal.h"
#include "common/goal_map.h"
#include "common/serialization.h"

class ChunksAvailabilityState {
public:
	enum State {
		kSafe,
		kEndangered,
		kLost
	};

	uint64_t safeChunks(uint8_t goal) const {
		return safeChunks_[goal];
	}
	uint64_t endangeredChunks(uint8_t goal) const {
		return endangeredChunks_[goal];
	}
	uint64_t lostChunks(uint8_t goal) const {
		return lostChunks_[goal];
	}
	void addChunk(uint8_t goal, State state) {
		getMapForState(state)[goal]++;
	}
	void removeChunk(uint8_t goal, State state) {
		getMapForState(state)[goal]--;
	}

private:
	// count of safe/endangered/lost chunks for each goal
	GoalMap<uint64_t> safeChunks_;
	GoalMap<uint64_t> endangeredChunks_;
	GoalMap<uint64_t> lostChunks_;

	GoalMap<uint64_t>& getMapForState(State state) {
		switch (state) {
		case kSafe:
			return safeChunks_;
		case kEndangered:
			return endangeredChunks_;
		case kLost:
			return lostChunks_;
		default:
			mabort("invalid state");
		}
	}

	friend uint32_t serializedSize(const ChunksAvailabilityState&);
	friend void serialize(uint8_t**, const ChunksAvailabilityState&);
	friend void deserialize(const uint8_t**, uint32_t&, ChunksAvailabilityState&);
};

class ChunksReplicationState {
public:
	static constexpr uint32_t kMaxPartsCount = kMaxXorLevel + 1;

	uint64_t chunksToReplicate(uint8_t goal, uint32_t missingParts) const {
		sassert(missingParts <= kMaxPartsCount);
		return chunksToReplicate_[goal][missingParts];
	}
	uint64_t chunksToDelete(uint8_t goal, uint32_t redundantParts) const {
		sassert(redundantParts <= kMaxPartsCount);
		return chunksToDelete_[goal][redundantParts];
	}
	void addChunk(uint8_t goal, uint32_t missingParts, uint32_t redundantParts) {
		missingParts = std::min(missingParts, kMaxPartsCount);
		redundantParts = std::min(redundantParts, kMaxPartsCount);
		chunksToReplicate_[goal][missingParts]++;
		chunksToDelete_[goal][redundantParts]++;
	}
	void removeChunk(uint8_t goal, uint32_t missingParts, uint32_t redundantParts) {
		missingParts = std::min(missingParts, kMaxPartsCount);
		redundantParts = std::min(redundantParts, kMaxPartsCount);
		chunksToReplicate_[goal][missingParts]--;
		chunksToDelete_[goal][redundantParts]--;
	}
private:
	// count of chunks that need replication/deletion for each goal and number of missing parts
	GoalMap<uint64_t[kMaxPartsCount + 1]> chunksToReplicate_;
	GoalMap<uint64_t[kMaxPartsCount + 1]> chunksToDelete_;

	friend uint32_t serializedSize(const ChunksReplicationState&);
	friend void serialize(uint8_t**, const ChunksReplicationState&);
	friend void deserialize(const uint8_t**, uint32_t&, ChunksReplicationState&);
};

inline uint32_t serializedSize(const ChunksAvailabilityState& state) {
	return serializedSize(state.safeChunks_)
		+ serializedSize(state.endangeredChunks_)
		+ serializedSize(state.lostChunks_);
}

inline void serialize(uint8_t** destination, const ChunksAvailabilityState& state) {
	serialize(destination, state.safeChunks_);
	serialize(destination, state.endangeredChunks_);
	serialize(destination, state.lostChunks_);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		ChunksAvailabilityState& state) {
	deserialize(source, bytesLeftInBuffer, state.safeChunks_);
	deserialize(source, bytesLeftInBuffer, state.endangeredChunks_);
	deserialize(source, bytesLeftInBuffer, state.lostChunks_);
}

inline uint32_t serializedSize(const ChunksReplicationState& state) {
	return serializedSize(state.chunksToReplicate_)
		+ serializedSize(state.chunksToDelete_);
}

inline void serialize(uint8_t** destination, const ChunksReplicationState& state) {
	serialize(destination, state.chunksToReplicate_);
	serialize(destination, state.chunksToDelete_);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		ChunksReplicationState& state) {
	deserialize(source, bytesLeftInBuffer, state.chunksToReplicate_);
	deserialize(source, bytesLeftInBuffer, state.chunksToDelete_);
}
