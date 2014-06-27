#pragma once

#include "common/platform.h"

#include <ostream>
#include <gtest/gtest.h>

#include "common/chunk_type.h"

inline std::ostream& operator<<(std::ostream& out, const ChunkType& chunkType) {
	out << chunkType.toString();
	return out;
}
