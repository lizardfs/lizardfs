#pragma once

#include "config.h"

#include <map>
#include <memory>

#include "common/chunk_connector.h"
#include "unittests/mocks/module_mock.h"

class ChunkConnectorMock : public ChunkConnector {
public:
	typedef std::map<NetworkAddress, ModuleMock*> Modules;

	ChunkConnectorMock(std::initializer_list<Modules::value_type> modules);
	~ChunkConnectorMock() {}
	int startUsingConnection(const NetworkAddress& server, const Timeout& timeout) const;

private:
	Modules modules_;
};
