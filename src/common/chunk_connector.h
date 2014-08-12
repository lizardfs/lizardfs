#pragma once

#include "common/platform.h"

#include "common/connection_pool.h"
#include "common/sockets.h"
#include "common/time_utils.h"

class ChunkConnector {
public:
	ChunkConnector(uint32_t sourceIp = 0);
	virtual ~ChunkConnector() {}
	virtual int startUsingConnection(const NetworkAddress& server, const Timeout& timeout) const;
	virtual void endUsingConnection(int fd, const NetworkAddress& server) const;

	/// A setter.
	void setRoundTripTime(uint32_t roundTripTime_ms) {
		roundTripTime_ms_ = roundTripTime_ms;
	}

	/// A setter.
	void setSourceIp(uint32_t sourceIp) {
		sourceIp_ = sourceIp;
	}

private:
	/// Time after which SYN packet will be considered lost during the first retry of tcptoconnect.
	uint32_t roundTripTime_ms_;

	/// IP address to bind to when connecting chunkservers.
	uint32_t sourceIp_;
};

class Connection {
public:
	Connection(ChunkConnector& connector, NetworkAddress server, const Timeout& timeout)
			: connector_(connector),
			  server_(std::move(server)),
			  fd_(-1) {
		fd_ = connector.startUsingConnection(server, timeout);
	}

	Connection(Connection&& other)
			: connector_(other.connector_),
			  server_(std::move(other.server_)),
			  fd_(std::move(other.fd_)) {
		other.fd_ = -1;
	}

	~Connection() {
		if (fd_ != -1) {
			destroy();
		}
	}

	int fd() const {
		sassert(fd_ != -1);
		return fd_;
	}

	void destroy() {
		sassert(fd_ != -1);
		tcpclose(fd_);
		fd_ = -1;
	}

	void endUsing() {
		sassert(fd_ != -1);
		connector_.endUsingConnection(fd_, server_);
	}

private:
	ChunkConnector& connector_;
	NetworkAddress server_;
	int fd_;
};

class ChunkConnectorUsingPool : public ChunkConnector {
public:
	static const uint32_t kConnectionPoolTimeout_s = 3;

	ChunkConnectorUsingPool(ConnectionPool& connectionPool, uint32_t sourceIp = 0);
	virtual int startUsingConnection(const NetworkAddress& server, const Timeout& timeout) const;
	virtual void endUsingConnection(int fd, const NetworkAddress& server) const;

private:
	ConnectionPool& connectionPool_;
};
