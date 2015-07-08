/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

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

#include "common/io_limiting.h"

/**
 * Implements full mechanism of limiting replication I/O bandwidth
 * providing the simplest possible API
 */
class ReplicationBandwidthLimiter {
public:
	/**
	 * Constructor
	 */
	ReplicationBandwidthLimiter();

	/**
	 * Sets the current limit of replication
	 * \param limit_kBps - limit in kibibytes in second
	 */
	void setLimit(uint64_t limit_kBps);

	/**
	 * Removes any limit
	 */
	void unsetLimit();

	/**
	 * Performs a wait for requested operation size
	 * \param requestedSize size of data requested to replicate in bytes
	 * \param timeout maximum allowed waiting time
	 * \return status of the operation
	 */
	uint8_t wait(uint64_t requestedSize, const SteadyDuration timeout);

private:
	/**
	 * A very simple limiter which manages one specific limit: kReplicationGroupId
	 */
	class ReplicationLimiter : public ioLimiting::Limiter {
	public:
		/**
		 * See the base class description
		 */
		uint64_t request(const IoLimitGroupId& groupId, uint64_t size) override;

		/**
		 * Sets current limit of replication in database
		 * \param limit_kBps - limit in kibibytes in second
		 */
		void setLimit(uint64_t limit_kBps);

		/**
		 * Removes replication limit from database
		 */
		void unsetLimit();

		// This implementation doesn't use registerReconfigure()

	private:
		/// A database holding and managing limits (only one in this class)
		IoLimitsDatabase database_;
	};

	/// An instance of replication limiter
	ReplicationLimiter limiter_;

	ioLimiting::RTClock clock_;
	ioLimiting::SharedState state_;
	std::unique_ptr<ioLimiting::Group> group_;

	/// A mutex for waiting operations
	static std::mutex mutex_;
};
