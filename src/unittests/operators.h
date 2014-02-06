#pragma once

#include <gtest/gtest.h>
#include <ostream>

#include "common/chunk_type.h"

inline std::ostream& operator<<(std::ostream& out, const ChunkType& chunkType) {
	out << chunkType.toString();
	return out;
}
