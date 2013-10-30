#ifndef LIZARDFS_TESTS_COMMON_OPERATORS_H_
#define LIZARDFS_TESTS_COMMON_OPERATORS_H_

#include <ostream>

#include "common/chunk_type.h"

std::ostream& operator<< (std::ostream& out, ChunkType chunkType) {
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


#endif // LIZARDFS_TESTS_COMMON_OPERATORS_H_
