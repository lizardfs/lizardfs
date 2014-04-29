#pragma once

#include "config.h"

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
