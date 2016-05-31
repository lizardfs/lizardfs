#pragma once

#include "common/platform.h"
#include "common/id_pool.h"

#ifdef LIZARDFS_HAVE_JUDY
#include <Judy.h>
#include <iterator>
#else
#include <stdint.h>
#include <algorithm>
#include <deque>
#endif

#include <cassert>
#include <list>

namespace detail {

#ifdef LIZARDFS_HAVE_JUDY

/*! \brief Implementation of SparseBitset based on Judy array.
 *
 * All set,unset function are time efficient (basically O(1) time complexity).
 * Extremely memory efficient if ranges of ids are set (can be as efficient as <2 bits per id).
 * With random ids it takes around ~64 bits per id.
 */
class SparseBitset {
	static_assert(sizeof(std::size_t) == sizeof(Word_t),
	              "Proper version of JudyArray required");

public:
	class const_iterator {
	public:
		typedef std::forward_iterator_tag iterator_category;
		typedef std::size_t               value_type;
		typedef std::ptrdiff_t            difference_type;
		typedef const value_type          &reference;
		typedef const value_type          *pointer;

	public:
		const_iterator() : data_(), pos_() {
		}

		const_iterator(Pvoid_t data, std::size_t p) : data_(data), pos_(p) {
		}

		const_iterator &operator++() {
			if (!data_) {
				return *this;
			}

			JError_t error;
			int      value;
			Word_t   pos = pos_;

			value = Judy1Next(data_, &pos, &error);
			if (value == JERR) {
				throw std::runtime_error("Judy: internal error");
			}

			if (!value) {
				data_ = NULL;
			} else {
				pos_ = pos;
			}

			return *this;
		}

		const_iterator operator++(int) {
			const_iterator it(*this);
			operator++();
			return it;
		}

		std::size_t operator*() {
			return pos_;
		}

		bool operator==(const const_iterator &it) {
			return data_ == it.data_ && (!data_ || pos_ == it.pos_);
		}

		bool operator!=(const const_iterator &it) {
			return !(*this == it);
		}

	protected:
		Pvoid_t data_;
		std::size_t pos_;
	};

public:
	SparseBitset() : data_() {
	}

	~SparseBitset() {
		if (data_) {
			Judy1FreeArray(&data_, NULL);
		}
	}

	bool test(std::size_t pos) const {
		return Judy1Test(data_, pos, NULL);
	}

	bool set(std::size_t pos) {
		JError_t error;
		int value;

		value = Judy1Set(&data_, pos, &error);
		checkError(value, error);

		return value;
	}

	bool reset(std::size_t pos) {
		JError_t error;
		int value;

		value = Judy1Unset(&data_, pos, &error);
		checkError(value, error);

		return value;
	}

	bool unset(std::size_t pos) {
		return reset(pos);
	}

	bool empty() const {
		return data_ == NULL;
	}

	const_iterator begin() const {
		JError_t error;
		int value;
		Word_t pos = 0;

		value = Judy1First(data_, &pos, &error);
		checkError(value, error);

		return value ? const_iterator(data_, pos) : const_iterator();
	}

	const_iterator end() const {
		return const_iterator();
	}

private:
	void checkError(int value, const JError_t &error) const {
		if (value == JERR) {
			if (JU_ERRNO(&error) == JU_ERRNO_NOMEM) {
				throw std::bad_alloc();
			}
			throw std::runtime_error("Judy: internal error");
		}
	}

protected:
	Pvoid_t data_;
};

#else

/*! \brief Implementation of SparseBitset based on std::dequeue.
 *
 * It's meant to be used if Judy array isn't available.
 *
 * It's quite memory efficient because it uses slightly more than 32 bits per id.
 * Unfortunately test and unset methods are slow (O(n) complexity).
 */
class SparseBitset {
public:
	typedef std::deque<uint32_t>::const_iterator const_iterator;

public:
	SparseBitset() : data_() {
	}

	bool test(std::size_t pos) const {
		auto it = std::find(data_.begin(), data_.end(), pos);
		return it != data_.end();
	}

	bool set(std::size_t pos) {
		data_.push_back(pos);
		return true;
	}

	bool reset(std::size_t pos) {
		auto it = std::find(data_.begin(), data_.end(), pos);
		if (it != data_.end()) {
			data_.erase(it);
			return true;
		}
		return false;
	}

	bool unset(std::size_t pos) {
		return reset(pos);
	}

	std::size_t size() const {
		return data_.size();
	}

	bool empty() const {
		return data_.empty();
	}

	const_iterator begin() const {
		return data_.begin();
	}

	const_iterator end() const {
		return data_.end();
	}

protected:
	std::deque<uint32_t> data_;
};

#endif

}  // detail

/*! \brief Implementation of id pool detainer.
 *
 * This class is responsible for holding released ids for at least defined time before they can be
 * reused. Comparing to IdPool each release and acquire action must have specified time stamp
 * with current time. This value is used to manage detention/release of ids.
 *
 * Simple approach to implementation would be to store time stamp with each detained id.
 * However this wouldn't be too memory efficient. Instead we split detention time to n buckets
 * holding ids for 1/n of whole time. For example for 24h detention time we can create 24 buckets
 * and in each bucket we keep ids from the same hour. This reduced by half memory usage.
 * Of course we loose time accuracy so we can't release id exactly after 24h. But the purpose of this
 * class is not to keep for exactly specified time but for at least this time. So if we
 * release some id after 25h hours instead of 24h then it isn't a problem.
 *
 * Data in each bucket are kept in SparseBitset. We have 2 implementations for this helper class.
 * One is based on std::dequeue and just keeps 32 bit ids.
 * Second method is much more sophisticated and uses Judy array to store 64 bit ids.
 * Judy array can store ids as efficiently as <2 bits per id and it has much more efficient
 * functions for searching, inserting and removing of ids.
 */
template <typename IDT, typename TT>
class IdPoolDetainer : protected IdPool<IDT> {
	typedef IdPool<IDT> base;

protected:
	struct BucketType {
		TT                   ts;
		detail::SparseBitset data;
	};

public:
	using base::nullId;

	typedef TT                    TimeType;
	typedef typename base::IdType IdType;

	struct EntryType {
		IdType   id;
		TimeType ts;
	};

	/*! \brief Iterator class that allows to walk through all detained ids. */
	class const_iterator {
	public:
		typedef std::forward_iterator_tag iterator_category;
		typedef EntryType value_type;
		typedef std::ptrdiff_t difference_type;
		typedef const value_type &reference;
		typedef const value_type *pointer;

	protected:
		typedef typename std::list<BucketType>::const_iterator const_iterator1;
		typedef detail::SparseBitset::const_iterator const_iterator2;

	public:
		const_iterator() {
		}

		explicit const_iterator(const const_iterator1 &i1, const const_iterator1 &i1e,
		                        const const_iterator2 &i2)
		    : i1_(i1), i1e_(i1e), i2_(i2) {
		}

		const_iterator &operator++() {
			assert(!i1_->data.empty());
			++i2_;
			if (i2_ == i1_->data.end()) {
				++i1_;
				if (i1_ != i1e_) {
					i2_ = i1_->data.begin();
				} else {
					i2_ = const_iterator2();
				}
			}

			return *this;
		}

		const_iterator operator++(int) {
			const_iterator it(*this);
			operator++();
			return it;
		}

		EntryType operator*() {
			return EntryType{IdType(*i2_), i1_->ts};
		}

		bool operator==(const const_iterator &it) {
			return i1_ == it.i1_ && (i1_ == i1e_ || i2_ == it.i2_);
		}

		bool operator!=(const const_iterator &it) {
			return !(*this == it);
		}

	protected:
		const_iterator1 i1_, i1e_;
		const_iterator2 i2_;
	};

public:
	/*! Constructor.
	 *
	 * \param detain_time   Time to hold ids in detention.
	 * \param bucket_count  Numbers of buckets that we split detention time to.
	 * \param max_size      Range of ids in the pool is from 1 to max_size
	 * \param max_detention_size Maximum number of ids that can be detained.
	 * \param block_size    Size of the bit block in bits
	 * \param cache_size    Size of the hash map cache used to reduced queries to bit blocks.
	 * \param release_count_ Number of ids that we try to remove from detention with each call
	 *                      to acquire/release.
	 */
	IdPoolDetainer(const TimeType &detain_time, const TimeType &bucket_count,
	               std::size_t max_size, std::size_t max_detention_size,
	               std::size_t block_size = 8 * 1024, std::size_t cache_size = 1024,
	               std::size_t release_count = 10)
	    : base(max_size, block_size, cache_size),
	      detain_time_(detain_time),
	      bucket_time_(detain_time / bucket_count),
	      max_detention_size_(max_detention_size),
	      detained_count_(0),
	      release_count_(release_count)  {
	}

	/*! \brief Returns id from the pool.
	 *
	 * This function returns unassigned id from the pool.
	 * Doesn't try to remove any ids from the detention.
	 *
	 * \return 0   - pool is full (no free elements available)
	 *         >=1 - id
	 */
	IdType acquire() {
		IdType id = base::acquire();

		if (id == nullId) {
			if (detention_.empty()) {
				return id;
			}

			BucketType &bucket(detention_.front());

			assert(!bucket.data.empty());
			std::size_t nid = *bucket.data.begin();
			if (bucket.data.unset(nid)) {
				--detained_count_;
			}

			if (bucket.data.empty()) {
				detention_.pop_front();
			}

			return IdType(nid);
		}

		return id;
	}

	/*! \brief Returns id from the pool.
	 *
	 * This function returns unassigned id from the pool.
	 *
	 * \param  ts  current time stamp.
	 * \return 0   - pool is full (no free elements available)
	 *         >=1 - id
	 */
	IdType acquire(const TimeType &ts) {
		releaseDetained(ts, release_count_);
		return acquire();
	}

	/*! \brief Returns the given ID (obtained via \p acquire) to the pool.
	 *
	 * \param id         Id to release.
	 * \param ts         current time stamp
	 * \param skip_check If true function doesn't check if the given id is already in detention.
	 *                   This may lead to invalid state if the id is released more than once.
	 *                   But if the caller can guarantee that this won't happen then it can
	 *                   save a lot of computations (specially if the SparseBitset doesn't have
	 *                   efficient test function).
	 *
	 * \return true  - operation successful.
	 *         false - id was already in the pool.
	 */
	bool release(const IdType &id, const TimeType &ts, bool skip_check = false) {
		releaseDetained(ts, release_count_);

		if (id == nullId || base::checkIfAvailable(id)) {
			return false;
		}

		if (!skip_check) {
			std::size_t nid = static_cast<std::size_t>(id);
			for (auto &bucket : detention_) {
				if (bucket.data.test(nid)) {
					return false;
				}
			}
		}

		if (detainedCount() >= max_detention_size_) {
			forceReleaseDetained(detainedCount() - max_detention_size_ + 1);
		}
		insert(id, ts);

		return true;
	}

	/*! \brief Mark specified id as acquired.
	 *
	 * Function doesn't try to release detained ids.
	 * Caller should care with using this function. In the worst case this function might
	 * search all detained ids to find id to mark. If the SparseBitset doesn't have
	 * efficient unset function this might be really slow.
	 *
	 * \param id         Id to release.
	 *
	 * \return true  - operation was successful.
	 *         false - id was already acquired.
	 */
	bool markAsAcquired(const IdType &id) {
		bool ret = base::markAsAcquired(id);

		if (ret) {
			return true;
		}

		std::size_t nid = static_cast<std::size_t>(id);
		for(auto it = detention_.begin(); it != detention_.end(); ++it) {
			if (it->data.unset(nid)) {
				--detained_count_;
				if (it->data.empty()) {
					detention_.erase(it);
				}
				return true;
			}
		}

		return false;
	}

	/*! \brief Mark specified id as acquired.
	 *
	 * \param id         Id to release.
	 *
	 * \return true  - operation was successful.
	 *         false - id was already acquired.
	 */
	bool markAsAcquired(const IdType &id, const TimeType &ts) {
		releaseDetained(ts, release_count_);
		return markAsAcquired(id);
	}

	/*! \brief Detain specified id.
	 *
	 * \param id         Id to detain.
	 * \param ts         current time stamp.
	 * \param overcrowd  If true then function doesn't remove any ids if detention is full.
	 * \param skip_check If true function doesn't check if the given id is already in detention.
	 *                   This may lead to invalid state if the id is detained more than once.
	 *                   But if the caller can guarantee that this won't happen then it can
	 *                   save a lot of computations (if the SparseBitset doesn't have
	 *                   efficient test function).
	 *
	 * \return true  - operation was successful.
	 *         false - id was already detained or acquired.
	 */
	bool detain(const IdType &id, const TimeType &ts, bool overcrowd = false, bool skip_check = false) {
		releaseDetained(ts, release_count_);

		if (!skip_check) {
			std::size_t nid = static_cast<std::size_t>(id);
			for (auto &bucket : detention_) {
				if (bucket.data.test(nid)) {
					return false;
				}
			}
		}

		if (!base::markAsAcquired(id)) {
			return false;
		}

		if (!overcrowd && detainedCount() >= max_detention_size_) {
			forceReleaseDetained(detainedCount() - max_detention_size_ + 1);
		}
		insert(id, ts);

		return true;
	}

	/*! \brief Try to release ids from detention.
	 *
	 * \param ts         current time stamp.
	 * \param max_count  Maximum number of ids that function tries to release from detention.
	 *
	 * \return Number of ids that were released.
	 */
	std::size_t releaseDetained(const TimeType &ts, int max_count) {
		std::size_t count = 0;

		while (!detention_.empty() && max_count > 0) {
			BucketType &bucket(detention_.front());

			if ((bucket.ts + bucket_time_ + detain_time_) >= ts) {
				break;
			}

			assert(!bucket.data.empty());
			std::size_t nid = *bucket.data.begin();
			bool r = bucket.data.unset(nid);

			assert(r);
			(void)r;

			++count;
			--detained_count_;
			base::release(IdType(nid));

			if (bucket.data.empty()) {
				detention_.pop_front();
			}

			--max_count;
		}

		return count;
	}

	/*! \brief Number of ids in detention. */
	std::size_t detainedCount() const {
		return detained_count_;
	}

	/*! \brief Number of acquired ids. */
	std::size_t size() const {
		return base::size() - detainedCount();
	}

	const_iterator begin() const {
		if (detention_.begin() != detention_.end()) {
			return const_iterator(detention_.begin(), detention_.end(),
			                      detention_.front().data.begin());
		}
		return const_iterator(detention_.begin(), detention_.end(),
		                      detail::SparseBitset::const_iterator());
	}

	const_iterator end() const {
		return const_iterator(detention_.end(), detention_.end(),
		                      detail::SparseBitset::const_iterator());
	}

	using base::maxSize;

protected:
	void insert(const IdType &id, const TimeType &ts) {
		if (detention_.empty() || (detention_.back().ts + bucket_time_) < ts) {
			detention_.emplace_back();
			detention_.back().ts = ts;
		}

		if (detention_.back().data.set(id)) {
			detained_count_++;
		}
	}

	void forceReleaseDetained(int count) {
		while (!detention_.empty() && count > 0) {
			BucketType &bucket(detention_.front());

			assert(!bucket.data.empty());
			std::size_t nid = *bucket.data.begin();
			bool r = bucket.data.unset(nid);

			assert(r);
			(void)r;

			--count;
			--detained_count_;
			base::release(IdType(nid));

			if (bucket.data.empty()) {
				detention_.pop_front();
			}
		}
	}

protected:
	std::list<BucketType> detention_;
	TimeType              detain_time_;
	TimeType              bucket_time_;
	std::size_t           max_detention_size_;
	std::size_t           detained_count_;
	std::size_t           release_count_;
};
