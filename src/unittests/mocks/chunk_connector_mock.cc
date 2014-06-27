#include "common/platform.h"
#include "unittests/mocks/chunk_connector_mock.h"

ChunkConnectorMock::ChunkConnectorMock(std::initializer_list<Modules::value_type> modules)
		: ChunkConnector(0),
		  modules_(std::move(modules)) {
	for (auto& module : modules_) {
		module.second->init();
	}
}

int ChunkConnectorMock::startUsingConnection(const NetworkAddress& server,
		const Timeout& timeout) const {
	auto it = modules_.find(server);
	if (it != modules_.end()) {
		return ChunkConnector::startUsingConnection(
				NetworkAddress(0x7f000001, it->second->port()), timeout);
	} else {
		throw ChunkserverConnectionException("No mock provided for server", server);
	}
}
