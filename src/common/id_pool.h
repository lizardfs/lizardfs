#pragma once
#include "common/platform.h"

#include <forward_list>
#include <unordered_set>
#include <vector>

#include "common/compact_vector.h"

namespace detail {

/*! \brief Helper class for IdPool.
 *
 * This class is almost typical bit pool. The only difference is that
 * when pool is full or empty underlaying vector storage releases all data.
 */
template<typename V,typename S>
class IdPoolBlock {
public:
	typedef V ValueType;
	typedef S SizeType;

public:
	/*! \brief Constructor.
	 *
	 * \param size amount of elements to keep in pool.
	 */
	IdPoolBlock(SizeType size)
	    : data_(),
	      max_size_(size),
	      free_count_(size),
	      next_pack_(0) {
	}

	/*! \brief Move constructor. */
	IdPoolBlock(IdPoolBlock &&block) noexcept
	    : data_(std::move(block.data_)),
	      max_size_(block.max_size_),
	      free_count_(block.free_count_),
	      next_pack_(block.next_pack_) {
		      block.free_count_ = max_size_;
		      block.next_pack_  = 0;
	}

	IdPoolBlock& operator=(IdPoolBlock &&block) noexcept {
		data_       = std::move(block.data_);
		max_size_   = block.max_size_;
		free_count_ = block.free_count_;
		next_pack_  = block.next_pack_;

		block.free_count_ = max_size_;
		block.next_pack_  = 0;

		return *this;
	}

	/*! \brief Function returns element id from pool. */
	SizeType acquire() {
		if (free_count_ > 0 && data_.size() == 0) {
			allocBlock(true);
		}

		assert(free_count_ > 0);

		// we use next_pack_ to speed up search for set bit
		SizeType start_pack = next_pack_;
		while (data_[next_pack_] == 0) {
			++next_pack_;
			if (next_pack_ >= data_.size()) {
				next_pack_ = 0;
			}

			if (next_pack_ == start_pack) {
				throw std::runtime_error("IdPool: memory corruption detected");
			}
		}

		int p = ffs(data_[next_pack_]) - 1;
		ValueType mask = static_cast<ValueType>(1) << p;

		assert(p >= 0 && data_[next_pack_] & mask);

		data_[next_pack_] ^= mask;
		free_count_--;

		if (free_count_ == 0) {
			data_.clear();
		}

		return next_pack_ * sizeof(ValueType) * 8 + p;
	}

	/*! \brief Mark element as already taken from pool. */
	bool markAsAcquired(SizeType id) {
		assert(id < max_size_);

		if (data_.size() == 0) {
			if (free_count_ == 0) {
				return false;
			}
			allocBlock(true);
		}

		SizeType pack = id / (8 * sizeof(ValueType));
		SizeType bit = id % (8 * sizeof(ValueType));
		ValueType mask = static_cast<ValueType>(1) << bit;

		if (!(data_[pack] & mask)) {
			return false;
		}

		data_[pack] ^= mask;
		free_count_--;

		if (free_count_ == 0) {
			data_.clear();
		}

		return true;
	}

	/*! \brief Returns element to the pool. */
	bool release(SizeType id) {
		if (data_.size() == 0) {
			if (free_count_ > 0) {
				return false;
			}
			allocBlock(false);
		}

		SizeType pack = id / (8 * sizeof(ValueType));
		SizeType bit = id % (8 * sizeof(ValueType));
		ValueType mask = static_cast<ValueType>(1) << bit;

		if (data_[pack] & mask) {
			return false;
		}

		data_[pack] |= mask;
		if (free_count_ == 0) {
			next_pack_ = pack;
		}
		free_count_++;

		if (free_count_ == max_size_) {
			data_.clear();
		}

		return true;
	}

	/*! \brief Check if element is available. */
	bool checkIfAvailable(SizeType id) const noexcept {
		if (data_.size() == 0) {
			return free_count_ > 0;
		}

		SizeType pack = id / (8 * sizeof(ValueType));
		SizeType bit = id % (8 * sizeof(ValueType));
		ValueType mask = static_cast<ValueType>(1) << bit;

		return data_[pack] & mask;
	}

	SizeType count() const noexcept {
		return free_count_;
	}

protected:
	/*! \brief Find First Set bit.
	 *
	 * Function uses binary search to find first set bit.
	 *
	 * \return 0 - no bit found
	 *         >0 - index of set bit counted from 1
	 */
	int ffs(ValueType r) {
		static const int t[16] = {-(int)(8 * sizeof(ValueType) - 4), 1, 2, 1, 3, 1, 2, 1, 4,
		                          1, 2, 1, 3, 1, 2, 1};

		int bit = 0;

		for (int bit_test = 8 * sizeof(ValueType) / 2; bit_test >= 4; bit_test /= 2) {
			ValueType mask = (static_cast<ValueType>(1) << bit_test) - 1;

			if (!(r & mask)) {
				bit += bit_test;
				r = r >> bit_test;
			}
		}

		return bit + t[r & 0xF];
	}

	/*! \brief Allocate data for block.
	 *
	 *  \param full If true function creates full pool.
	 *              If false function creates empty pool.
	 */
	void allocBlock(bool full) {
		SizeType rsize = (max_size_ + 8 * sizeof(ValueType) - 1) / (8 * sizeof(ValueType));
		SizeType last_bits = max_size_ % (8 * sizeof(ValueType));

		assert(rsize <= data_.max_size());

		if (full) {
			assert(free_count_ == max_size_);

			data_.resize(rsize, ~static_cast<ValueType>(0));
			if (last_bits > 0) {
				data_.back() = (static_cast<ValueType>(1) << last_bits) - 1;
			}
			next_pack_ = 0;
		} else {
			assert(free_count_ == 0);

			data_.resize(rsize, 0);
		}
	}

protected:
	compact_vector<ValueType> data_;
	SizeType                  max_size_;
	SizeType                  free_count_;
	SizeType                  next_pack_;
};

}  // detail

/*! Implementation of id pool (bit pool).
 *
 * This is highly optimized implementation of bit pool.
 * All the functions have amortized constant time cost (O(1)).
 * If the markAsAcquired function isn't used then the cost is plain constant.
 *
 * Also memory usage is reduced compared to simple bit pool implementation.
 * Whole id space is divided into blocks. When block is full (all bits set) or empty (all bits unset)
 * then memory for that block is freed.
 *
 * Element of the pool can be any type that can be explicitly converted
 * to and from type std::size_t. This is the only requirement.
 */
template <typename IDT>
class IdPool {
public:
	typedef IDT IdType;
	static const IdType nullId;

protected:
	typedef detail::IdPoolBlock<std::size_t,uint32_t> BlockType;

public:
	/*! Constructor.
	 *
	 * \param max_size Range of ids in the pool is from 1 to max_size
	 * \param block_size Size of the bit block in bits
	 * \param cache_size Size of the hash map cache used to reduced queries to bit blocks.
	 */
	IdPool(std::size_t max_size, std::size_t block_size = 8*1024, std::size_t cache_size = 1024)
	    : cache_(cache_size) {
		std::size_t rsize = 8 * sizeof(BlockType::ValueType);

		block_size = max_size < block_size ? max_size + 1 : block_size;
		cache_size = max_size < cache_size ? max_size + 1 : cache_size;

		block_size_      = rsize * ((block_size + rsize - 1) / rsize);
		max_cache_size_  = cache_size;
		max_id_          = max_size;

		max_block_count_ = (max_size / block_size_) + 1;

		free_list_last_  = free_list_.before_begin();

		// reserve id=0 for special use
		markAsAcquired(0);
		used_count_ = 0;
	}

	/*! \brief Returns id from the pool.
	 *
	 * \return 0   - pool is full (no free elements available)
	 *         >=1 - id
	 */
	IdType acquire() {
		if (!cache_.empty()) {
			IdType result = *cache_.begin();
			cache_.erase(cache_.begin());
			used_count_++;
			return result;
		}

		// remove empty blocks left by markAsAcquired
		while (!free_list_.empty()) {
			std::size_t block = free_list_.front();
			if (block_[block].count() > 0) {
				break;
			}
			listPopFront();
		}

		if (free_list_.empty()) {
			if (!addFreeBlocks(1)) {
				return nullId;
			}
		}

		std::size_t block = free_list_.front();
		IdType result = block_[block].acquire() + block * block_size_;
		if (block_[block].count() == 0) {
			listPopFront();
		}
		used_count_++;

		return result;
	}

	/*! \brief Returns the given ID (obtained via \p acquire) to the pool.
	 * \return true  - operation successful.
	 *         false - id was already in the pool.
	 */
	bool release(IdType id) {
		std::size_t nid = static_cast<std::size_t>(id);

		if (nid == 0 || nid > max_id_) {
			return false;
		}

		std::size_t block = nid / block_size_;
		std::size_t bid = nid % block_size_;

		if (block >= block_.size()) {
			return false;
		}

		if (cache_.size() < max_cache_size_) {
			if (cache_.find(nid) != cache_.end()) {
				return false;
			}
			if (block_[block].checkIfAvailable(bid)) {
				return false;
			}

			cache_.insert(nid);
			used_count_--;
			return true;
		}

		if (!block_[block].release(bid)) {
			return false;
		}
		if (block_[block].count() == 1) {
			listPushFront(block);
		}
		used_count_--;

		return true;
	}

	/*! \brief Mark specified id as acquired.
	 *
	 * \return true  - operation was successful.
	 *         false - id was already acquired.
	 */
	bool markAsAcquired(IdType id) {
		std::size_t nid = static_cast<std::size_t>(id);

		auto icache = cache_.find(nid);
		if (icache != cache_.end()) {
			cache_.erase(icache);
			used_count_++;
			return true;
		}

		std::size_t block = nid / block_size_;
		std::size_t bid   = nid % block_size_;

		if (block >= block_.size()) {
			if (!addFreeBlocks(block + 1 - block_.size())) {
				return false;
			}
		}

		if (block_[block].markAsAcquired(bid)) {
			used_count_++;

			// If block gets exhausted we should remove it from free list.
			// But it would require to search list (complexity O(N)).
			// Instead we try to guess that we need to remove block from front
			// of the list. If that is not the case we will later remove block
			// in the acquire function. This way we have amortized O(1) cost.
			while (!free_list_.empty()) {
				std::size_t block = free_list_.front();
				if (block_[block].count() > 0) {
					break;
				}
				listPopFront();
			}

			return true;
		}

		return false;
	}

	/*! \brief Check if id can be acquired.
	 *
	 * \return true  - id is available.
	 *         false - id was already acquired.
	 */
	bool checkIfAvailable(IdType id) noexcept {
		std::size_t nid = static_cast<std::size_t>(id);

		if (nid == 0 || nid > max_id_) {
			return false;
		}

		if (cache_.find(nid) != cache_.end()) {
			return true;
		}

		std::size_t block = nid / block_size_;
		std::size_t bid = nid % block_size_;

		if (block >= block_.size()) {
			return true;
		}

		return block_[block].checkIfAvailable(bid);
	}

	std::size_t maxSize() const noexcept {
		return max_id_;
	}

	std::size_t size() const noexcept {
		return used_count_;
	};

protected:
	bool addFreeBlocks(int count) {
		if (count <= 0 || (block_.size() + count) > max_block_count_) {
			return false;
		}

		block_.reserve(block_.size() + count);
		for (int i = 0; i < count; i++) {
			std::size_t left = max_id_ - block_size_ * block_.size();
			block_.emplace_back(left < block_size_ ? left + 1 : block_size_);

			// we add block to the end of free list so blocks with lower number
			// of free elements are used first
			listPushBack(block_.size() - 1);
		}

		return true;
	}

	void listPushBack(std::size_t block) {
		free_list_last_ = free_list_.emplace_after(free_list_last_, block);
	}

	void listPopFront() {
		if (free_list_last_ == free_list_.begin()) {
			free_list_.pop_front();
			free_list_last_ = free_list_.before_begin();
		} else {
			free_list_.pop_front();
		}
	}

	void listPushFront(std::size_t block) {
		free_list_.emplace_front(block);
		if (free_list_last_ == free_list_.before_begin()) {
			free_list_last_ = free_list_.begin();
		}
	}

protected:
	std::unordered_set<std::size_t>          cache_;
	std::vector<BlockType>                   block_;
	std::forward_list<std::size_t>           free_list_;
	std::forward_list<std::size_t>::iterator free_list_last_;
	std::size_t                              used_count_;
	std::size_t                              max_id_, max_cache_size_;
	std::size_t                              max_block_count_, block_size_;
};

template<typename IdType>
const IdType IdPool<IdType>::nullId = IdType();
