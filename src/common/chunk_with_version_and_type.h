#pragma once

#include "config.h"

#include <cstdint>
#include <iomanip>
#include <sstream>
#include <tuple>

#include "common/chunk_type.h"
#include "common/serialization.h"

struct ChunkWithVersionAndType {
	uint64_t id;
	uint32_t version;
	ChunkType type;

	ChunkWithVersionAndType() : id(0), version(0), type(ChunkType::getStandardChunkType()) {}

	ChunkWithVersionAndType(uint64_t id, uint32_t version, ChunkType type)
			: id(id),
			  version(version),
			  type(type) {
	}

	std::string toString() const {
		std::stringstream ss;
		ss << std::hex << std::setfill('0');
		ss << std::setw(16) << id << '_';
		ss << std::setw(8) << version;
		ss << " (" << type.toString() << ")";
		return ss.str();
	}

	bool operator<(const ChunkWithVersionAndType& other) const {
		return std::make_tuple(id, version, type)
			< std::make_tuple(other.id, other.version, other.type);
	}
	bool operator==(const ChunkWithVersionAndType& other) const {
		return id == other.id && version == other.version && type == other.type;
	}
};

inline uint32_t serializedSize(const ChunkWithVersionAndType& chunk) {
	return serializedSize(chunk.id, chunk.version, chunk.type);
}

inline void serialize(uint8_t** destination, const ChunkWithVersionAndType& chunk) {
	return serialize(destination, chunk.id, chunk.version, chunk.type);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		ChunkWithVersionAndType& chunk) {
	return deserialize(source, bytesLeftInBuffer, chunk.id, chunk.version, chunk.type);
}
