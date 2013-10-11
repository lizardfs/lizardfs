#ifndef CONNECTION_POOL_H_
#define CONNECTION_POOL_H_

#include <chrono>
#include <list>
#include <map>
#include <mutex>

#include "mfscommon/network_address.h"

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
				validUntil_(Clock::now() + std::chrono::seconds(timeout)) {
		}

		int fd() const {
			return fd_;
		}

		bool isValid() const {
			return Clock::now() < validUntil_;
		}

	private:
		// TODO(msulikowski) replace with steady_clock/monotonic_clock if available. This can be done
		// after merging with a build system which checks for availability of these features
		typedef std::chrono::system_clock Clock;
		typedef std::chrono::time_point<Clock> TimePoint;

		int fd_;
		TimePoint validUntil_;
	};

	typedef std::map<NetworkAddress, std::list<Connection>> ConnectionsContainer;

	std::mutex mutex_;
	ConnectionsContainer connections_;
};

#endif /* CONNECTION_POOL_H_ */
