#include "mfscommon/connection_pool.h"

#include "mfscommon/massert.h"
#include "mfscommon/sockets.h"

#include <vector>

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

void ConnectionPool::cleanup() {
	std::unique_lock<std::mutex> lock(mutex_);
	std::vector<int> descriptorsToClose;
	ConnectionsContainer::iterator keyAndConnectionListIt = connections_.begin();
	while (keyAndConnectionListIt != connections_.end()) {
		std::list<Connection>& connectionList = keyAndConnectionListIt->second;
		std::list<Connection>::iterator connectionIt = connectionList.begin();
		while (connectionIt != connectionList.end()) {
			if (!connectionIt->isValid()) {
				descriptorsToClose.push_back(connectionIt->fd());
				std::list<Connection>::iterator it = connectionIt;
				++connectionIt;
				connectionList.erase(it);
			} else {
				++connectionIt;
			}
		}
		if (connectionList.empty()) {
			ConnectionsContainer::iterator it = keyAndConnectionListIt;
			++keyAndConnectionListIt;
			connections_.erase(it);
		} else {
			++keyAndConnectionListIt;
		}
	}
	lock.unlock();
	for (int fd : descriptorsToClose) {
		tcpclose(fd);
	}
}
