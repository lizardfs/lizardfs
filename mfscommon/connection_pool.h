#ifndef CONNECTION_POOL_H_
#define CONNECTION_POOL_H_

#include <chrono>
#include <list>
#include <map>
#include <mutex>

class ConnectionPool {
public:
	typedef uint32_t IpAddress;
	typedef uint16_t Port;

	/**
	 * Returns descriptor if connection found in the pool, -1 otherwise
	 */
	int getConnection(IpAddress ip, Port port);

	/**
	 * Puts connection in the pool for future use.
	 * This connection will not be returned after the timeout has expired.
	 */
	void putConnection(int fd, IpAddress ip, Port port, int timeout);

private:
	typedef std::pair<IpAddress, Port> CacheKey;

	// TODO(msulikowski) replace with steady_clock/monotonic_clock if available. This can be done
	// after merging with a build system which checks for availability of these features
	typedef std::chrono::system_clock Clock;
	typedef std::chrono::time_point<Clock> TimePoint;

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
		int fd_;
		TimePoint validUntil_;
	};

	std::mutex mutex_;
	std::map<CacheKey, std::list<Connection>> connections_;
};

#endif /* CONNECTION_POOL_H_ */
