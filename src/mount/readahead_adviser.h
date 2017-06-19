/*
   Copyright 2016 Skytechnology sp. z o.o.

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

#include "common/ring_buffer.h"
#include "common/time_utils.h"
#include <array>
#include <cassert>

/*! Adviser class for readahead mechanism.
 *
 * This class is used to predict size of readahead requests on the basis of
 * sequentiality of read operations and estimated latency.
 */
class ReadaheadAdviser {
public:
	struct HistoryEntry {
		int64_t timestamp;
		uint32_t request_size;
	};

	ReadaheadAdviser(uint32_t timeout_ms, uint32_t window_size_limit = kDefaultWindowSizeLimit) :
		current_offset_(),
		window_(kInitWindowSize),
		random_candidates_(),
		max_window_size_(window_size_limit),
		window_size_limit_(window_size_limit),
		random_threshold_(kRandomThreshold),
		requested_bytes_(),
		timer_(),
		timeout_ms_(timeout_ms) {}

	/*!
	 * \brief Acknowledge read request and judge whether it is sequential or random.
	 * \param offset offset of read operation
	 * \param size size of read operation
	 */
	void feed(uint64_t offset, uint32_t size) {
		if (timeout_ms_ == 0) {
			window_ = 0;
			return;
		}
		addToHistory(size);
		if (offset == current_offset_) {
			random_candidates_ = 0;
			expand();
			current_offset_ = offset + size;
		} else {
			random_candidates_++;
			if (looksRandom()) {
				reduce();
				current_offset_ = offset + size;
			}
		}
	}

	/*!
	 * \brief Count suggested readahead window size.
	 * \return suggested readahead window size
	 */
	int window() const {
		return std::min(window_, max_window_size_);
	}

private:

	/*!
	 * \brief Check if history entry is not overdue.
	 * \return true if history entry is overdue
	 */
	bool expired(HistoryEntry entry, int64_t timestamp) {
		return entry.timestamp + kHistoryEntryLifespan_ns < timestamp;
	}

	/*!
	 * \brief Add size of read request to history.
	 * \param size size of read request
	 */
	void addToHistory(uint32_t size) {
		int64_t timestamp = timer_.elapsed_us();
		// Remove stale history entries
		while (history_.full() || (!history_.empty() && expired(history_.front(), timestamp))) {
			requested_bytes_ -= history_.front().request_size;
			history_.pop_front();
		}

		history_.push_back(HistoryEntry{timestamp, size});
		requested_bytes_ += size;

		// If there is enough data in history to predict max window size, do it
		if (history_.size() >= kHistoryValidityThreshold  && timestamp != history_.front().timestamp) {
			adjustMaxWindowSize(timestamp);
		}
	}

	/*!
	 * \brief Adjust max window size on the basis of estimated latency.
	 * \param timestamp time point used for latency estimation
	 */
	void adjustMaxWindowSize(int64_t timestamp) {
		double throughput_MBps = (double)requested_bytes_ / (timestamp - history_.front().timestamp);
		// Max window size is set on the basis of estimated throughput
		max_window_size_ = std::min<uint64_t>(window_size_limit_, 2 * throughput_MBps * timeout_ms_ * 1024);
		max_window_size_ = std::max(max_window_size_, kInitWindowSize);
	}

	/*!
	 * \brief Increase window size.
	 */
	void expand() {
		if (window_ >= max_window_size_) {
			return;
		}

		if (window_ < max_window_size_ / 16) {
			window_ *= 4;
		} else {
			window_ *= 2;
		}
	}

	/*!
	 * \brief Decrease window size.
	 */
	void reduce() {
		if (window_ >= 2 * kInitWindowSize) {
			window_ /= 2;
		}
	}

	/*!
	 * \brief Check if read operations seem to be random.
	 */
	bool looksRandom() {
		return random_candidates_ > random_threshold_;
	}

	static const unsigned kInitWindowSize = 1 << 16;
	static const unsigned kDefaultWindowSizeLimit = 1 << 22;
	static const int kRandomThreshold = 3;
	static const int kHistoryEntryLifespan_ns = 1 << 20;
	static const int kHistoryCapacity = 64;
	static const unsigned kHistoryValidityThreshold = 3;
	static_assert(kHistoryCapacity >= (int)kHistoryValidityThreshold,
			"History validity threshold must not be greater than history capacity");

	uint64_t current_offset_;
	unsigned window_;
	int random_candidates_;

	unsigned max_window_size_;
	unsigned window_size_limit_;
	int random_threshold_;

	RingBuffer<HistoryEntry, kHistoryCapacity> history_;
	uint64_t requested_bytes_;
	Timer timer_;
	uint32_t timeout_ms_;
};
