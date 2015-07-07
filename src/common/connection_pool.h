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

#include <chrono>
#include <list>
#include <map>
#include <mutex>

#include "common/network_address.h"
#include "common/time_utils.h"

class ConnectionPool {
public:

	/**
	 * Returns descriptor if connection found in the pool, -1 otherwise
	 */
	int getConnection(const NetworkAddress& address);

	/**
	 * Puts connection in the pool for future use.
	 * This connection will not be returned after the timeout has expired.
	 */
	void putConnection(int fd, const NetworkAddress& address, int timeout);

	/**
	 * Removes timed out connections from the pool
	 */
	void cleanup();

private:
	class Connection {
	public:
		Connection(int fd, int timeout) :
				fd_(fd),
				validUntil_(std::chrono::seconds(timeout)) {
		}

		int fd() const {
			return fd_;
		}

		bool isValid() const {
			return !validUntil_.expired();
		}

	private:
		int fd_;
		Timeout validUntil_;
	};

	typedef std::map<NetworkAddress, std::list<Connection>> ConnectionsContainer;

	std::mutex mutex_;
	ConnectionsContainer connections_;
};
