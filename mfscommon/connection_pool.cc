#include "mfscommon/connection_pool.h"

#include "mfscommon/massert.h"
#include "mfscommon/sockets.h"

void ConnectionPool::putConnection(int fd, IpAddress ip, Port port, int timeout) {
	std::unique_lock<std::mutex> lock(mutex_);
	connections_[CacheKey(ip, port)].push_back(Connection(fd, timeout));
}

int ConnectionPool::getConnection(IpAddress ip, Port port) {
	CacheKey cacheKey = CacheKey(ip, port);
	while (true) {
		std::unique_lock<std::mutex> lock(mutex_);
		auto connectionsIterator = connections_.find(cacheKey);
		if (connectionsIterator == connections_.end()) {
			return -1;
		}
		std::list<Connection>& openConnections = connectionsIterator->second;
		if (openConnections.empty()) {
			return -1;
		}
		Connection connection = openConnections.front();
		openConnections.pop_front();
		lock.unlock();
		if (connection.isValid()) {
			return connection.fd();
		} else {
			tcpclose(connection.fd());
		}
	}
}
