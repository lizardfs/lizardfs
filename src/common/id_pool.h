#pragma once
#include "common/platform.h"

#include <stdexcept>
#include <type_traits>
#include <vector>

#include "common/exception.h"

/// Base class for all exceptions thrown by this class
LIZARDFS_CREATE_EXCEPTION_CLASS(IdPoolException, Exception);

LIZARDFS_CREATE_EXCEPTION_CLASS_MSG(OutOfIdsIdPoolException, IdPoolException,
		"no more IDs available in pool");

LIZARDFS_CREATE_EXCEPTION_CLASS_MSG(UnknownIdIdPoolException, IdPoolException,
		"unknown ID");

/// A class which maintains a pool of IDs.
/// \p IdType should be integral, e.g. uint16_t.
/// poolId can be any random number and is used to make ID from different pools incompatible.
template <typename IdType, int poolId = 0>
class IdPool {
public:
	static_assert(std::is_integral<IdType>::value, "IdPool accepts only integral IdType");

	typedef std::vector<bool> FreeIdList;

	/// The type of identifiers from this pool.
	class Id {
	public:
		IdType value() const {
			return value_;
		}

		bool operator<(const Id& other) const {
			return value_ < other.value_;
		}

		bool operator==(const Id& other) const {
			return value_ == other.value_;
		}

		bool operator!=(const Id& other) const {
			return value_ != other.value_;
		}

	private:
		Id(IdType value) : value_(value) {}

		IdType value_;

		friend class IdPool;
	};

	/// A constructor.
	/// \param size  maximum number of allocated IDs
	IdPool(FreeIdList::size_type size)
			: isIdUsed_(size + 1, false),
			  freeIdCount_(size),
			  nextIdIndex_(0) {
		isIdUsed_[nullId().value()] = true;
	}

	/// Gets a new, unused ID from the pool
	Id get() {
		if (freeIdCount_ == 0) {
			throw OutOfIdsIdPoolException();
		}
		while (isIdUsed_[nextIdIndex_] == true) {
			nextIdIndex_ = (nextIdIndex_ + 1) % isIdUsed_.size();
		}
		freeIdCount_--;
		isIdUsed_[nextIdIndex_] = true;
		return Id(nextIdIndex_);
	}

	/// Returns the given ID (obtained via \p get) to the pool.
	void put(Id id) {
		if (id == nullId()) {
			throw UnknownIdIdPoolException();
		}
		try {
			if (!isIdUsed_.at(id.value())) {
				throw UnknownIdIdPoolException();
			}
			freeIdCount_++;
			isIdUsed_.at(id.value()) = false;
		} catch (std::out_of_range&) {
			throw UnknownIdIdPoolException();
		}
	}

	/// Number of free and taken IDs
	FreeIdList::size_type size() const {
		return isIdUsed_.size() - 1;
	}

	/// Returns a special ID which is never returned by \p get.
	static const Id nullId() {
		return Id(0);
	}

private:
	/// A dict: id value -> bool
	FreeIdList isIdUsed_;

	/// Number of free IDs in the pool
	FreeIdList::size_type freeIdCount_;

	/// Used by \p get to speed up ID allocation
	FreeIdList::size_type nextIdIndex_;
};

