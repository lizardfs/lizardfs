#pragma once

#include <gtest/gtest.h>
#include <ostream>

#include "common/chunk_type.h"

inline std::ostream& operator<<(std::ostream& out, const ChunkType& chunkType) {
	if (chunkType.isStandardChunkType()) {
		out << "standard";
	} else {
		if (chunkType.isXorParity()) {
			out << "xor_parity_of_" << static_cast<unsigned>(chunkType.getXorLevel());
		} else {
			out << "xor_" << static_cast<unsigned>(chunkType.getXorPart())
					<< "_of_" << static_cast<unsigned>(chunkType.getXorLevel());
		}
	}
	return out;
}
