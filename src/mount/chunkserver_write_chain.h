#ifndef CHUNKSERVER_WRITE_CHAIN_H
#define CHUNKSERVER_WRITE_CHAIN_H

#include "common/massert.h"
#include "common/network_address.h"
#include "common/sockets.h"

#include <vector>

/*
 * A chain of chunkservers used to replicate the data
 */
class ChunkserverWriteChain {
public:
	ChunkserverWriteChain();
	~ChunkserverWriteChain();

	/**
	 * Adds a new chunkserver to the chain
	 */
	void add(const NetworkAddress& server);

	/**
	 * Connects to the first chunkserver from a (non-empty) chain.
	 * Returns socket decriptor or -1 in case of an error.
	 */
	int connect();

	/**
	 * Creates a message forming the chain, which should be sent to the
	 * first chunkserver from the chain.
	 */
	void createInitialMessage(std::vector<uint8_t>& message, uint64_t chunkId, uint32_t version);

	size_t size() const {
		return servers_.size();
	}

	NetworkAddress head() const {
		eassert(!servers_.empty());
		return servers_.front();
	}

private:
	int createNewChunkserverConnection(const NetworkAddress& server);

	std::vector<NetworkAddress> servers_;
};

#endif /* CHUNKSERVER_WRITE_CHAIN_H */
