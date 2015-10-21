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

#include <array>
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <map>
#include <numeric>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include "common/media_label.h"

/*!
 * \brief This file contains definitions of classes describing goals.
 *
 * Goals consist of slices, which are a way of describing allocation of data to chunkservers.
 *
 * Slice has a defined SliceType (eg. std, xor3) and a description of distribution of data
 * over chunkservers.
 * SliceType determines a number of parts a slice has.
 * - 'std' type (standard chunk) consists of 1 part: all data
 * - 'xor2' type consists of 3 parts: xor part no1, xor part no2 and parity
 * - 'xor5' type consists of 6 parts: 5 xor parts and parity
 *
 * For each part of a slice, a client can configure number of its copies
 * on particular labels.
 *
 * Examples:
 *
 * Simple goal: each chunk should be kept in 2 copies on chunkserver A,
 * 1 copy on chunkserver B, and 1 copy on any chunkserver
 * +================+
 * |      Goal      |
 * +================+
 * | Slice 1: std   |
 * +----------------+
 * | A: 2 B: 1 _: 1 |
 * +----------------+
 *
 *
 * Complex goal:
 * 1. each chunk should be kept in 2 whole copies on A and 3 copies anywhere
 * 2. each chunk should be divided into xor2 parts, and:
 *   - 1 of the parts should be kept in two copies on chunkserver A
 *   - another one should be kept in two copies - one on A, one on B
 *   - yet another part should be kept in 2 copies anywhere
 * 3. each chunk should be also divided into xor3 parts, and:
 *   - 1 of the parts should be kept on chunkserver A
 *   - another one should be kept on B
 *   - yet another one on C
 *   - yet yet another one on D
 * +==============+===============+===============+
 * |                     Goal                     |
 * +==============+===============+===============+
 * | Slice 1: std | Slice 2: xor2 | Slice 3: xor3 |
 * +==============+===============+===============+
 * | A: 2 _: 3    | A: 2          | A: 1          |
 * +--------------+---------------+---------------+
 *                | A: 1 B: 1     | B: 1          |
 *                +---------------+---------------+
 *                | _: 2          | C: 1          |
 *                +---------------+---------------+
 *                                | D: 1          |
 *                                +---------------+
 *
 */


namespace detail {

/*! \brief This class describes the way a single replica of chunk is distributed over chunkservers.
 * It can also be stored integrally on one server or be sliced into several pieces.
 */
class SliceType {
public:
	struct hash {
		std::size_t operator()(const SliceType &type) const {
			return static_cast<std::size_t>(static_cast<int>(type));
		}
	};

	/// available SliceTypes
	enum { kStandard = 0,
	       kTape,
	       kXor2 = 2,
	       kXor3,
	       kXor4,
	       kXor5,
	       kXor6,
	       kXor7,
	       kXor8,
	       kXor9 };

	static const int kTypeCount = 10;

	explicit SliceType(int value)
		: value_(value) {
	};

	bool operator==(const SliceType& other) const {
		return value_ == other.value_;
	}

	bool operator!=(const SliceType& other) const {
		return value_ != other.value_;
	}

	bool operator<(const SliceType& other) const {
		return value_ < other.value_;
	}

	explicit operator int() const {
		return value_;
	}

	SliceType& operator=(const int value) {
		value_ = value;
		return *this;
	}

	int expectedParts() const {
		assert(isValid());
		return kTypeParts[value_];
	}

	std::string toString() const {
		assert(isValid());
		return kTypeNames[value_];
	}

	bool isValid() const {
		return value_ >= kStandard && value_ < kTypeCount;
	}

private:
	static bool validTypeValue(int value) {
		return value >= kStandard && value < kTypeCount;
	}

	static const int kTypeParts[kTypeCount];
	static const std::array<std::string, kTypeCount> kTypeNames;

	int value_;
};

/*! \brief This class contains information about how many copies
 * every part of specific slice type should have and about media labels
 * that they should be stored on.
 */
class Slice {
public:
	typedef detail::SliceType Type;
	typedef std::map<MediaLabel, int> Labels;
	typedef std::vector<Labels>::iterator iterator;
	typedef std::vector<Labels>::const_iterator const_iterator;

	// WARNING: Changing this value can cause a breakage of metadata backward compatibility.
	static constexpr int kMaxPartsCount = 11;

	/// Constructor based on type
	explicit Slice(Type type)
		: type_(std::move(type)),
		  data_(type_.expectedParts()) {
	}
	Slice(const Slice &other) : type_(other.type_), data_(other.data_) {}
	Slice(Slice &&other) noexcept : type_(std::move(other.type_)), data_(std::move(other.data_)) {}

	Slice &operator=(const Slice &other) {
		type_ = other.type_;
		data_ = other.data_;
		return *this;
	}

	Slice &operator=(Slice &&other) noexcept {
		type_ = std::move(other.type_);
		data_ = std::move(other.data_);
		return *this;
	}

	int getExpectedCopies() const;

	iterator begin() {
		return data_.begin();
	}

	iterator end() {
		return data_.end();
	}

	const_iterator begin() const {
		return data_.begin();
	}

	const_iterator end() const {
		return data_.end();
	}

	Type getType() const {
		return type_;
	}

	/*! \brief How many parts are in this slice. */
	int size() const {
		return data_.size();
	}

	/*! \brief Merge in another Slice - they have to be of the same type and size */
	void mergeIn(const Slice& other);

	/*!
	 * \brief Component is valid if size is as expected,
	 * and there is at least one label for each part.
	 */
	bool isValid() const;

	/*! \brief Access labels for specific part */
	Labels& operator[](int part) {
		assert(part >= 0 && part < (int)data_.size());
		return data_[part];
	}

	/*! \brief Access labels for specific part */
	const Labels& operator[](int part) const {
		assert(part >= 0 && part < (int)data_.size());
		return data_[part];
	}

	bool operator==(const Slice& other) const {
		return type_ == other.type_ && data_ == other.data_;
	}

	bool operator!=(const Slice& other) const {
		return type_ != other.type_ || data_ != other.data_;
	}

	/*! \brief How many labels are in the Labels structure */
	static int countLabels(const Labels& labels) {
		return std::accumulate(labels.begin(), labels.end(), 0,
			[](int sum, const Labels::value_type& elem) {
				return sum + elem.second;
			});
	}

private:
	/*! \brief Union (like set union) of two Labels structures */
	static Labels getLabelsUnion(const Labels& first, const Labels& second);

	static int labelsDistance(const Labels& first, const Labels& second);

	Type type_;
	std::vector<Labels> data_;
};

} // namespace detail

/*! \brief Class representing a goal id */
class GoalId {
public:
	static constexpr uint8_t kMin = 1;
	static constexpr uint8_t kMax = 40;

	explicit GoalId(uint8_t value)
		: value_(value) {
	}

	explicit operator uint8_t() const {
		return value_;
	}

	bool isValid() const {
		return isValid(value_);
	}

	static bool isValid(uint8_t id) {
		return kMin <= id && id <= kMax;
	}

private:
	uint8_t value_;
};

/*! \brief Class which represents a description of a goal. */
class Goal {
public:
	typedef detail::Slice Slice;
	typedef std::unordered_map<Slice::Type, Slice, Slice::Type::hash> SliceContainer;

	struct iterator : public SliceContainer::iterator {
		iterator(SliceContainer::iterator it) :
			SliceContainer::iterator(it) {
		}
		SliceContainer::value_type::second_type &operator*() {
			return SliceContainer::iterator::operator*().second;
		}
		SliceContainer::value_type::second_type *operator->() {
			return &(operator*());
		}
	};

	struct const_iterator : public SliceContainer::const_iterator {
		const_iterator(SliceContainer::const_iterator it) :
			SliceContainer::const_iterator(it) {
		}
		const_iterator(SliceContainer::iterator it) :
			SliceContainer::const_iterator(it) {
		}

		const SliceContainer::value_type::second_type &operator*() {
			return SliceContainer::const_iterator::operator*().second;
		}
		const SliceContainer::value_type::second_type *operator->() {
			return &(operator*());
		}
	};

	/*! \brief  Maximum number of copies a goal can require. */
	static constexpr uint32_t kMaxExpectedCopies = 30;

	Goal() = default;

	explicit Goal(std::string name)
		: name_(name) {
	}

	Goal(const Goal &) = default;
	Goal(Goal &&) = default;

	Goal &operator=(const Goal &) = default;
	Goal &operator=(Goal &&) = default;

	/*! \brief Set specific slice of the Goal */
	void setSlice(Slice slice) {
		// slightly longer implementation to avoid unnecessary call to default constructor
		auto it_slice = goal_slices_.find(slice.getType());
		if (it_slice != goal_slices_.end()) {
			it_slice->second = std::move(slice);
		} else {
			Slice::Type type = slice.getType();
			goal_slices_.insert({std::move(type), std::move(slice)});
		}
	}

	/*! \brief Current goal becomes union of itself with otherGoal */
	void mergeIn(const Goal& otherGoal);

	/*! \brief Get iterator to slice regarding specific slice type of this goal object. */
	iterator find(const Slice::Type &sliceType) {
		return goal_slices_.find(sliceType);
	}
	const_iterator find(const Slice::Type &sliceType) const {
		return goal_slices_.find(sliceType);
	}

	/*! \brief Verifies names of goals. */
	static bool isNameValid(const std::string& goalName) {
		// Let's use exactly the same algorithm as for media labels
		return MediaLabelManager::isLabelValid(goalName);
	}

	int getExpectedCopies() const;

	Slice& operator[](const Slice::Type& type) {
		auto it = goal_slices_.find(type);
		if (it == goal_slices_.end()) {
			return goal_slices_.insert({type, Slice(type)}).first->second;
		}
		return it->second;
	}

	const Slice &operator[](const Slice::Type &type) const {
		auto it = goal_slices_.find(type);
		if (it == goal_slices_.end()) {
			throw std::out_of_range("No such slice");
		}
		return it->second;
	}

	int size() const {
		return goal_slices_.size();
	}

	const std::string& getName() const {
		return name_;
	}

	void setName(const std::string &name) {
		name_ = name;
	}

	iterator begin() {
		return goal_slices_.begin();
	}

	iterator end() {
		return goal_slices_.end();
	}

	const_iterator begin() const {
		return goal_slices_.begin();
	}

	const_iterator end() const {
		return goal_slices_.end();
	}

	/// Operator ==.
	bool operator==(const Goal& other) const {
		return name_ == other.name_
				&& goal_slices_ == other.goal_slices_;
	}

	bool operator!=(const Goal& other) const {
		return name_ != other.name_
				|| goal_slices_ != other.goal_slices_;
	}

private:
	/*! \brief Name of the goal. */
	std::string name_;
	/*! \brief Slices of the goal */
	SliceContainer goal_slices_;
};

inline std::string to_string(const Goal::Slice::Type& type) {
	return type.toString();
}

std::string to_string(const Goal::Slice& slice);

std::string to_string(const Goal& goal);

