#pragma once

#include "common/platform.h"

#include <ostream>
#include <gtest/gtest.h>

#include "common/chunk_type.h"
#include "common/read_planner.h"

inline std::ostream& operator<<(std::ostream& out, const ChunkType& chunkType) {
	out << chunkType.toString();
	return out;
}

inline void PrintTo(const ReadPlanner::ReadOperation& op, std::ostream* out) {
	*out << "read(" << op.requestOffset << ", " << op.requestSize << ") : "
			<< ::testing::PrintToString(op.readDataOffsets);
}

inline void PrintTo(const ReadPlanner::PostProcessOperation& op, std::ostream* out) {
	*out << "  dest = " << op.destinationOffset << ", src = " << op.sourceOffset
			<< ", xor = " << ::testing::PrintToString(op.blocksToXorOffsets);
}

inline void PrintTo(const ReadPlanner::Plan& plan, std::ostream* out) {
	*out << "ReadPlanner::Plan";
	*out << "\n  bufferSize: " << plan.requiredBufferSize;
	*out << "\n  basic read operations:";
	for (const auto& op : plan.basicReadOperations) {
		*out << "\n    " << ::testing::PrintToString(op.first) <<
				" - " << ::testing::PrintToString(op.second);
	}
	*out << "\n  additional read operations:";
	for (const auto& op : plan.additionalReadOperations) {
		*out << "\n    " << ::testing::PrintToString(op.first) <<
				" - " << ::testing::PrintToString(op.second);
	}
	*out << "\n  postprocessing for all finished:";
	for (const auto& op : plan.getPostProcessOperationsForBasicPlan()) {
		*out << "\n    " << ::testing::PrintToString(op);
	}
	std::set<ChunkType> usedParts;
	for (const auto& partAndOperation : plan.getAllReadOperations()) {
		usedParts.insert(partAndOperation.first);
	}
	for (ChunkType part : usedParts) {
		if (plan.isReadingFinished({part})) {
			*out << "\n  postprocessing for {" << part << "} unfinished:";
			for (const auto& op : plan.getPostProcessOperationsForExtendedPlan({part})) {
				*out << "\n    " << ::testing::PrintToString(op);
			}
		}
	}
}
