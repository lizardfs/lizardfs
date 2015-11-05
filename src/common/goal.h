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
#include "common/flat_map.h"
#include "common/flat_set.h"
#include "common/small_vector.h"
#include "common/vector_range.h"

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

	explicit SliceType(int value) : value_(value) {
	}

	bool operator==(const SliceType &other) const {
		return value_ == other.value_;
	}

	bool operator!=(const SliceType &other) const {
		return value_ != other.value_;
	}

	bool operator<(const SliceType &other) const {
		return value_ < other.value_;
	}

	explicit operator int() const {
		return value_;
	}

	SliceType &operator=(const int value) {
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

/*! \brief Forward iterator for accessing Slice parts. */
template <class Proxy, class DataContainer, class SizeContainer>
class SliceIterator {
public:
	typedef std::forward_iterator_tag iterator_category;
	typedef Proxy value_type;
	typedef std::ptrdiff_t difference_type;
	typedef const value_type *pointer;
	typedef const value_type &reference;

	SliceIterator(int part, std::size_t offset, SizeContainer &part_size,
	              DataContainer &data) noexcept : part_(part),
	                                              offset_(offset),
	                                              part_size_(part_size),
	                                              data_(data) {
	}

	SliceIterator(const SliceIterator &other) noexcept : part_(other.part_),
	                                                     offset_(other.offset_),
	                                                     part_size_(other.part_size_),
	                                                     data_(other.data_) {
	}

	value_type operator*() const noexcept {
		return value_type(vector_range<DataContainer, typename SizeContainer::value_type>(
		    data_, offset_, part_size_[part_]));
	}

	SliceIterator &operator++() noexcept {
		offset_ += part_size_[part_];
		++part_;
		return *this;
	}

	SliceIterator operator++(int) noexcept {
		SliceIterator temp(*this);
		offset_ += part_size_[part_];
		++part_;
		return temp;
	}

	bool operator==(const SliceIterator &other) const noexcept {
		return part_ == other.part_;
	}

	bool operator!=(const SliceIterator &other) const noexcept {
		return part_ != other.part_;
	}

protected:
	int part_;
	std::size_t offset_;
	SizeContainer &part_size_;
	DataContainer &data_;
};

/*! \brief Class storing information about Slice
 *
 * This class contains information about how many copies
 * every part of specific slice type should have and about media labels
 * that they should be stored on.
 *
 * Storage optimization:
 *
 * First version of Slice used std::vector + std::map for data management.
 * For each slice part we had map for storing pair (label,count) and all maps were
 * kept in vector.
 *
 * Slice (vector of maps)
 * part 0: (map) ("A", 3), ("B", 1), ...
 * part 1: (map) ...
 * ...
 * part n: (map) ...
 *
 * This implementation puts enormous stress on memory allocator because
 * std::map allocated new object for each label+count pair stored in Slice.
 *
 * To improve efficiency of Slice we now use "extreme" flat storage.
 * This means that all data are kept in one vector.
 *
 * So lets assume that we have slice with following values:
 *
 * part 0: ("A0", 1), ("B0", 2)
 * part 1: ("A1", 3),
 * part 2: ("A2", 4), ("B2", 5), ("C2", 6)
 *
 * This is stored in vector as:
 *
 * ("A0", 1), ("B0", 2), ("A1", 3), ("A2", 4), ("B2", 5), ("C2", 6)
 *
 * To know where each part starts we use vector with size of each part. So we have
 *
 * Label data: ("A0", 1), ("B0", 2), ("A1", 3), ("A2", 4), ("B2", 5), ("C2", 6)
 * Part size:  2,                    1,         3
 *
 * This storage is memory efficient (usually we keep all data in one cache line),
 * but it's not too easy to use. To simplify access we implemented some special classes
 * to change interface to be more similar to previous version of Slice.
 *
 * First of all we use vector_range to make each Slice part visible as vector.
 * Then we encapsulate it in flat_map to make it behave like map. This type is
 * called PartProxy. There is also corresponding ConstPartProxy that gives access
 * to part but doesn't allow changes.
 *
 * To get PartProxy (or ConstPartProxy) object there is defined operator [] (taking part index).
 * Also it possible to use iterators (functions begin, end).
 *
 * This implementation introduces some restriction on iterators and accessing parts.
 * Any operation adding elements to part invalidates map iterators (for all parts).
 * Also adding elements to one PartProxy object invalidates proxies corresponding to other parts.
 *
 * To further reduce number of memory allocations we use small_vector instead of std::vector.
 */
class Slice {
public:
	// WARNING: Changing this value can cause a breakage of metadata backward compatibility.
	static constexpr int kMaxPartsCount = 11;

	typedef detail::SliceType Type;
	typedef small_vector<std::pair<MediaLabel, uint16_t>, 12> DataContainer;
	typedef std::array<uint16_t, kMaxPartsCount> SizeContainer;
	typedef flat_map<MediaLabel, uint16_t, DataContainer> Labels;
	typedef flat_map<MediaLabel, uint16_t,
	                 vector_range<DataContainer, SizeContainer::value_type>> PartProxy;
	typedef flat_map<MediaLabel, uint16_t,
	                 vector_range<const DataContainer, SizeContainer::value_type>> ConstPartProxy;
	typedef SliceIterator<PartProxy, DataContainer, SizeContainer> iterator;
	typedef SliceIterator<ConstPartProxy, const DataContainer, const SizeContainer> const_iterator;

	/// Constructor based on type
	explicit Slice(Type type) noexcept : type_(std::move(type)), part_size_(), data_() {
	}

	Slice(const Slice &other)
	    : type_(other.type_), part_size_(other.part_size_), data_(other.data_) {
	}

	Slice(Slice &&other) noexcept : type_(std::move(other.type_)),
	                                part_size_(std::move(other.part_size_)),
	                                data_(std::move(other.data_)) {
	}

	Slice &operator=(const Slice &other) {
		type_ = other.type_;
		part_size_ = other.part_size_;
		data_ = other.data_;
		return *this;
	}

	Slice &operator=(Slice &&other) noexcept {
		type_ = std::move(other.type_);
		part_size_ = std::move(other.part_size_);
		data_ = std::move(other.data_);
		return *this;
	}

	int getExpectedCopies() const;

	iterator begin() noexcept {
		return iterator(0, 0, part_size_, data_);
	}

	iterator end() noexcept {
		return iterator(size(), 0, part_size_, data_);
	}

	const_iterator begin() const noexcept {
		return const_iterator(0, 0, part_size_, data_);
	}

	const_iterator end() const noexcept {
		return const_iterator(size(), 0, part_size_, data_);
	}

	const_iterator cbegin() const noexcept {
		return const_iterator(0, 0, part_size_, data_);
	}

	const_iterator cend() const noexcept {
		return const_iterator(size(), 0, part_size_, data_);
	}

	Type getType() const  noexcept {
		return type_;
	}

	/*! \brief How many parts are in this slice. */
	int size() const noexcept {
		return type_.expectedParts();
	}

	/*! \brief Merge in another Slice - they have to be of the same type and size */
	void mergeIn(const Slice &other);

	/*!
	 * \brief Component is valid if size is as expected,
	 * and there is at least one label for each part.
	 */
	bool isValid() const noexcept;

	/*! \brief Access labels for specific part */
	PartProxy operator[](int part) noexcept {
		assert(part >= 0 && part < size());
		return PartProxy(vector_range<DataContainer, SizeContainer::value_type>(
		    data_, getPartOffset(part), part_size_[part]));
	}

	/*! \brief Access labels for specific part */
	ConstPartProxy operator[](int part) const noexcept {
		assert(part >= 0 && part < size());
		return ConstPartProxy(vector_range<const DataContainer, SizeContainer::value_type>(
		    data_, getPartOffset(part), part_size_[part]));
	}

	bool operator==(const Slice &other) const noexcept {
		return type_ == other.type_ && data_ == other.data_;
	}

	bool operator!=(const Slice &other) const noexcept {
		return type_ != other.type_ || data_ != other.data_;
	}

	/*! \brief How many labels are in the Labels structure */
	static int countLabels(const Labels &labels) noexcept {
		return std::accumulate(
		    labels.begin(), labels.end(), 0,
		    [](int sum, const Labels::value_type &elem) { return sum + elem.second; });
	}

	static int countLabels(const PartProxy &labels) noexcept {
		return std::accumulate(
		    labels.begin(), labels.end(), 0,
		    [](int sum, const DataContainer::value_type &entry) { return sum + entry.second; });
	}

	static int countLabels(const ConstPartProxy &labels) noexcept {
		return std::accumulate(
		    labels.begin(), labels.end(), 0,
		    [](int sum, const DataContainer::value_type &entry) { return sum + entry.second; });
	}

private:
	std::size_t getPartOffset(int part) const noexcept {
		return std::accumulate(part_size_.begin(), part_size_.begin() + part, (std::size_t)0);
	}

	/*! \brief Union (like set union) of two Labels structures */
	void makeLabelsUnion(Labels &result, const ConstPartProxy &first, const ConstPartProxy &second);

	static int labelsDistance(const ConstPartProxy &first, const Labels &second) noexcept;

	Type type_;
	SizeContainer part_size_;
	DataContainer data_;
};

}  // namespace detail

/*! \brief Class representing a goal id */
class GoalId {
public:
	static constexpr uint8_t kMin = 1;
	static constexpr uint8_t kMax = 40;

	explicit GoalId(uint8_t value) : value_(value) {
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
private:
	struct SliceCompare {
		bool operator()(const detail::Slice &a, const detail::Slice &b) const {
			return a.getType() < b.getType();
		}
		bool operator()(const detail::Slice &a, const detail::SliceType &b) const {
			return a.getType() < b;
		}
		bool operator()(const detail::SliceType &a, const detail::Slice &b) const {
			return a < b.getType();
		}
	};

public:
	typedef detail::Slice Slice;
	typedef flat_set<Slice,small_vector<Slice, 3>, SliceCompare> SliceContainer;
	typedef SliceContainer::iterator iterator;
	typedef SliceContainer::const_iterator const_iterator;

	/*! \brief  Maximum number of copies a goal can require. */
	static constexpr uint32_t kMaxExpectedCopies = 30;

	Goal() = default;

	explicit Goal(std::string name) : name_(name) {
	}

	Goal(const Goal &) = default;
	Goal(Goal &&) = default;

	Goal &operator=(const Goal &) = default;
	Goal &operator=(Goal &&) = default;

	/*! \brief Set specific slice of the Goal */
	void setSlice(Slice slice) {
		// slightly longer implementation to avoid unnecessary call to default constructor
		auto it_slice = find(slice.getType());
		if (it_slice != goal_slices_.end()) {
			*it_slice = std::move(slice);
		} else {
			goal_slices_.insert(std::move(slice));
		}
	}

	/*! \brief Current goal becomes union of itself with otherGoal */
	void mergeIn(const Goal &other_goal);

	/*! \brief Get iterator to slice regarding specific slice type of this goal object. */
	iterator find(const Slice::Type &slice_type) {
		return goal_slices_.find(slice_type, SliceCompare());
	}
	const_iterator find(const Slice::Type &slice_type) const {
		return goal_slices_.find(slice_type, SliceCompare());
	}

	/*! \brief Verifies names of goals. */
	static bool isNameValid(const std::string &goal_name) {
		// Let's use exactly the same algorithm as for media labels
		return MediaLabelManager::isLabelValid(goal_name);
	}

	int getExpectedCopies() const;

	Slice &operator[](const Slice::Type &type) {
		auto it = find(type);
		if (it == goal_slices_.end()) {
			return *goal_slices_.insert(Slice(type)).first;
		}
		return *it;
	}

	const Slice &operator[](const Slice::Type &type) const {
		auto it = find(type);
		if (it == goal_slices_.end()) {
			throw std::out_of_range("No such slice");
		}
		return *it;
	}

	int size() const {
		return goal_slices_.size();
	}

	const std::string &getName() const {
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
	bool operator==(const Goal &other) const {
		return name_ == other.name_ && goal_slices_ == other.goal_slices_;
	}

	bool operator!=(const Goal &other) const {
		return name_ != other.name_ || goal_slices_ != other.goal_slices_;
	}

private:
	/*! \brief Name of the goal. */
	std::string name_;
	/*! \brief Slices of the goal */
	SliceContainer goal_slices_;
};

inline std::string to_string(const Goal::Slice::Type &type) {
	return type.toString();
}

std::string to_string(const Goal::Slice &slice);

std::string to_string(const Goal &goal);
