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
