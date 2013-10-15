#include "read_operation_planner.h"

#include <algorithm>

#include "mfscommon/goal.h"
#include "mfscommon/MFSCommunication.h"

namespace {

class IsNotXorLevel {
public:
	IsNotXorLevel(ChunkType::XorLevel level, bool acceptParity = true)
			: level_(level),
			  acceptParity_(acceptParity) {
	}

	bool operator()(ChunkType chunkType) {
		if (!chunkType.isXorChunkType() || chunkType.getXorLevel() != level_) {
			return true;
		}
		if (!acceptParity_ && chunkType.isXorParity()) {
			return true;
		}
		return false;
	}

private:
	ChunkType::XorLevel level_;
	bool acceptParity_;
};

} // anonymous namespace

ReadOperationPlanner::ReadOperationPlanner(const std::vector<ChunkType>& parts) {
	partsToUse = parts;
	std::vector<ChunkType>& uniqueParts(partsToUse);
	std::sort(uniqueParts.begin(), uniqueParts.end());
	uniqueParts.erase(std::unique(uniqueParts.begin(), uniqueParts.end()), uniqueParts.end());

	bool isFullReplicaAvailable = false;
	std::vector<bool> isParityForLevelAvailable(kMaxXorLevel + 1, false);
	std::vector<int> partsForLevelAvailable(kMaxXorLevel + 1, 0);
	for (ChunkType chunkType : uniqueParts) {
		if (chunkType.isStandardChunkType()) {
			isFullReplicaAvailable = true;
		} else {
			sassert(chunkType.isXorChunkType());
			if (chunkType.isXorParity()) {
				isParityForLevelAvailable[chunkType.getXorLevel()] = true;
			} else {
				partsForLevelAvailable[chunkType.getXorLevel()]++;
			}
		}
	}

	// 1. If we can read from xor chunks without using a parity choose the highest level
	for (int level = kMaxXorLevel; level >= kMinXorLevel; --level) {
		if (partsForLevelAvailable[level] == level) {
			auto newEnd = std::remove_if(
					uniqueParts.begin(), uniqueParts.end(), IsNotXorLevel(level, false));
			uniqueParts.erase(newEnd, uniqueParts.end());
			return;
		}
	}

	// 2. If there is a full replica, choose it
	if (isFullReplicaAvailable) {
		uniqueParts = { ChunkType::getStandardChunkType() };
		return;
	}

	// 3. If there is a set of xor chunks with one missing and parity available, choose it
	for (int level = kMaxXorLevel; level >= kMinXorLevel; --level) {
		if (partsForLevelAvailable[level] == level - 1 && isParityForLevelAvailable[level]) {
			auto newEnd = std::remove_if(
					uniqueParts.begin(), uniqueParts.end(), IsNotXorLevel(level, true));
			uniqueParts.erase(newEnd, uniqueParts.end());
			return;
		}
	}

	// 4. Chunk is unreadable
	uniqueParts = std::vector<ChunkType>();
	return;
}

bool ReadOperationPlanner::isPossible() const {
	return !partsToUse.empty();
}

const std::vector<ChunkType>& ReadOperationPlanner::chosenParts() const {
	return partsToUse;
}

static ReadOperationPlanner::Plan createPlanForStandardChunkType(uint32_t offset, uint32_t size) {
	ReadOperationPlanner::ReadOperation read;
	read.offset = offset;
	read.size = size;
	for (uint32_t off = 0; off < size; off += MFSBLOCKSIZE) {
		read.offsetsOfBlocks.push_back(off);
	}
	ReadOperationPlanner::Plan plan;
	plan.readOperations[ChunkType::getStandardChunkType()] = read;
	plan.requiredBufferSize = size;
	return plan;
}

static ReadOperationPlanner::ReadOperation createXorReadOperationWithoutParity(
		ChunkType chunkType, uint32_t offset, uint32_t size) {
	ChunkType::XorLevel level = chunkType.getXorLevel();
	ChunkType::XorPart part = chunkType.getXorPart();
	ReadOperationPlanner::ReadOperation read;
	uint32_t blockBegin = offset / MFSBLOCKSIZE;
	uint32_t blockEnd = (offset + size) / MFSBLOCKSIZE;
	for (uint32_t block = blockBegin; block < blockEnd; ++block) {
		if ((block % level) + 1 == part) {
			read.offsetsOfBlocks.push_back(block * MFSBLOCKSIZE - offset);
		}
	}
	if (!read.offsetsOfBlocks.empty()) {
		uint32_t indexOfFirstBlockToRead = (offset + read.offsetsOfBlocks[0]) / MFSBLOCKSIZE;
		read.offset = (indexOfFirstBlockToRead / level) * MFSBLOCKSIZE;
		read.size = read.offsetsOfBlocks.size() * MFSBLOCKSIZE;
	} else {
		read.offset = read.size = 0;
	}
	return read;
}

ReadOperationPlanner::Plan ReadOperationPlanner::getPlanFor(uint32_t offset, uint32_t size) const {
	const std::vector<ChunkType>& chosenParts(partsToUse);
	sassert(!chosenParts.empty());
	sassert(offset % MFSBLOCKSIZE == 0);
	sassert(size % MFSBLOCKSIZE == 0);
	sassert(size > 0);
	sassert(offset + size <= MFSCHUNKSIZE);

	if (chosenParts[0].isStandardChunkType()) {
		sassert(chosenParts.size() == 1);
		return createPlanForStandardChunkType(offset, size);
	}

	// Create a plan for uncorrupted xor chunks
	Plan plan;
	ChunkType::XorLevel level = chosenParts[0].getXorLevel();
	uint32_t totalSize = 0;
	for (ChunkType chunkType : chosenParts) {
		sassert(chunkType.getXorLevel() == level);
		sassert(chunkType.isXorParity() == false);
		sassert(plan.readOperations.count(chunkType) == 0);
		ReadOperationPlanner::ReadOperation read =
				createXorReadOperationWithoutParity(chunkType, offset, size);
		if (read.size > 0) {
			totalSize += read.size;
			plan.readOperations[chunkType] = read;
		}
	}
	sassert(totalSize == size);
	plan.requiredBufferSize = size;
	return plan;
}
