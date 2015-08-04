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

#pragma once

#include "common/platform.h"

#include <ostream>
#include <gtest/gtest.h>

#include "common/chunk_part_type.h"
#include "common/read_planner.h"

inline std::ostream& operator<<(std::ostream& out, const ChunkPartType& chunkType) {
	out << chunkType.toString();
	return out;
}

inline void PrintTo(const ReadPlan::ReadOperation& op, std::ostream* out) {
	*out << "read(" << op.requestOffset << ", " << op.requestSize << ") : "
			<< ::testing::PrintToString(op.readDataOffsets);
}

inline void PrintTo(const ReadPlan::PostProcessOperation& op, std::ostream* out) {
	*out << "  dest = " << op.destinationOffset << ", src = " << op.sourceOffset
			<< ", xor = " << ::testing::PrintToString(op.blocksToXorOffsets);
}

inline void PrintTo(const ReadPlan& plan, std::ostream* out) {
	*out << "ReadPlan";
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
	std::set<ChunkPartType> usedParts;
	for (const auto& partAndOperation : plan.getAllReadOperations()) {
		usedParts.insert(partAndOperation.first);
	}
	for (ChunkPartType part : usedParts) {
		if (plan.isReadingFinished({part})) {
			*out << "\n  postprocessing for {" << part << "} unfinished:";
			for (const auto& op : plan.getPostProcessOperationsForExtendedPlan({part})) {
				*out << "\n    " << ::testing::PrintToString(op);
			}
		}
	}
}
