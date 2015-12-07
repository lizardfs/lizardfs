/*
   Copyright 2015 Skytechnology sp. z o.o.

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

#include <cstdint>
#include <sstream>
#include <string>

#include "common/serialization.h"
#include "common/goal.h"

namespace legacy {

class ChunkPartType {
public:
	static const int kMaxPartsCount = 11;
	static const int kMaxType = 9;

	/*! \brief Default constructor.
	 *
	 * Constructor sets value to standard chunk part type.
	 */
	ChunkPartType() : id_() {
	}

	/*! \brief Internal value constructor.
	 *
	 * \param id internal value representing chunk part type
	 */
	explicit ChunkPartType(int id) : id_(id) {
	}

	/*! \brief Value constructor.
	 *
	 * \param type slice type
	 * \param part slice part
	 */
	ChunkPartType(Goal::Slice::Type type, int part) {
		assert(part >= 0 && part < kMaxPartsCount);
		assert((int)type >= 0 && (int)type <= (kMaxType + 1));
		id_ = ((int)type * kMaxPartsCount) + part;
	}

	/*! \brief Function returns slice type. */
	Goal::Slice::Type getSliceType() const {
		return Goal::Slice::Type(id_ / kMaxPartsCount);
	}

	/*! \brief Function returns slice part. */
	int getSlicePart() const {
		return id_ % kMaxPartsCount;
	}

	/*! \brief Function returns internal representation of chunk type. */
	uint8_t getId() const {
		return id_;
	}

	/*! \brief Check if internal value represents valid chunk part type.
	 * \return true if internal value corresponds to valid chunk part type.
	 */
	bool isValid() const {
		return (int)getSliceType() <= kMaxType && getSlicePart() < getSliceType().expectedParts();
	}

	/*! \brief Function returns string describing chunk part type. */
	std::string toString() const {
		return to_string(getSliceType()) + ":" + std::to_string(getSlicePart());
	}

	bool operator==(const ChunkPartType &other) const {
		return id_ == other.id_;
	}

	bool operator!=(const ChunkPartType &other) const {
		return id_ != other.id_;
	}

	bool operator<(const ChunkPartType &other) const {
		return id_ < other.id_;
	}

	uint32_t serializedSize() const {
		return ::serializedSize(id_);
	}

	inline void serialize(uint8_t **destination) const {
		::serialize(destination, id_);
	}

	void deserialize(const uint8_t **source, uint32_t &bytes_left_in_buffer) {
		uint8_t value;
		::deserialize(source, bytes_left_in_buffer, value);
		id_ = value;
		if (!isValid()) {
			throw IncorrectDeserializationException("unknown chunk type id: " +
			                                        std::to_string(value));
		}
	}
private:
	uint8_t id_;
};

} // legacy

inline std::string to_string(legacy::ChunkPartType cpt) {
	return cpt.toString();
}

/*! \brief Chunk part type compact storage.
 *
 * This class represents all possible chunk part types.
 * Because it's used in each instance of class Chunk in master server
 * it's designed for maximum memory efficiency (only 1 byte).
 *
 * Each chunk part can have a type that is combination of slice type and slice part.
 *
 * If we denote slice type as 's' and slice part as 'p' then internal representation (id) is
 * calculated using following rule:
 *
 * id = m * s + p
 *
 * where m is equal to maximum number of slice parts for any slice type.
 */
class ChunkPartType {
public:
	static const int kMaxPartsCount = 64;
	static const int kMaxTypeCount = 2048;

	static_assert(Goal::Slice::kMaxPartsCount <= kMaxPartsCount, "unsupported number of parts");
	static_assert(Goal::Slice::Type::kTypeCount <= kMaxTypeCount, "unsupported number of types");

	/*! \brief Default constructor.
	 *
	 * Constructor sets value to standard chunk part type.
	 */
	ChunkPartType() : id_() {
	}

	/*! \brief Internal value constructor.
	 *
	 * \param id internal value representing chunk part type
	 */
	explicit ChunkPartType(int id) : id_(id) {
	}

	/*! \brief Value constructor.
	 *
	 * \param type slice type
	 * \param part slice part
	 */
	ChunkPartType(Goal::Slice::Type type, int part) {
		assert((int)type < kMaxTypeCount);
		assert(part >= 0 && part < kMaxPartsCount);
		id_ = ((int)type * kMaxPartsCount) + part;
	}

	ChunkPartType(const legacy::ChunkPartType& cpt) : id_() {
		id_ = ((int)cpt.getSliceType() * kMaxPartsCount) + cpt.getSlicePart();
	}

	/*! \brief Function returns slice type. */
	Goal::Slice::Type getSliceType() const {
		return Goal::Slice::Type(id_ / kMaxPartsCount);
	}

	/*! \brief Function returns slice part. */
	int getSlicePart() const {
		return id_ % kMaxPartsCount;
	}

	/*! \brief Function returns internal representation of chunk type. */
	int getId() const {
		return id_;
	}

	/*! \brief Check if internal value represents valid chunk part type.
	 * \return true if internal value corresponds to valid chunk part type.
	 */
	bool isValid() const {
		return getSliceType().isValid() && getSlicePart() < getSliceType().expectedParts();
	}

	/*! \brief Function returns string describing chunk part type. */
	std::string toString() const {
		return to_string(getSliceType()) + ":" + std::to_string(getSlicePart());
	}

	bool operator==(const ChunkPartType &other) const {
		return id_ == other.id_;
	}

	bool operator!=(const ChunkPartType &other) const {
		return id_ != other.id_;
	}

	bool operator<(const ChunkPartType &other) const {
		return id_ < other.id_;
	}

	explicit operator legacy::ChunkPartType () const {
		// if legacy::ChunkPartType can't represent new slice type
		// then we convert it to type (legacy::ChunkPartType::kMaxType + 1)
		// that legacy::ChunkPartType can represent and is considered
		// as invalid in old LizardFS versions.
		int type = std::min((int)getSliceType(), legacy::ChunkPartType::kMaxType + 1);
		int part = std::min(getSlicePart(), legacy::ChunkPartType::kMaxPartsCount - 1);
		return legacy::ChunkPartType(Goal::Slice::Type(type), part);
	}
private:
	uint16_t id_;

	friend uint32_t serializedSize(const ChunkPartType &);
	friend void serialize(uint8_t **, const ChunkPartType &);
	friend void deserialize(const uint8_t **, uint32_t &, ChunkPartType &);
};

inline uint32_t serializedSize(const ChunkPartType &chunk_id) {
	return serializedSize(chunk_id.id_);
}

inline void serialize(uint8_t **destination, const ChunkPartType &chunk_id) {
	serialize(destination, chunk_id.id_);
}

inline void deserialize(const uint8_t **source, uint32_t &bytes_left_in_buffer,
		ChunkPartType &chunk_id) {
	uint16_t value;
	deserialize(source, bytes_left_in_buffer, value);
	chunk_id.id_ = value;
	if (!chunk_id.isValid()) {
		throw IncorrectDeserializationException("unknown chunk type id: " +
		                                        std::to_string(value));
	}
}

inline std::string to_string(ChunkPartType cpt) {
	return cpt.toString();
}
