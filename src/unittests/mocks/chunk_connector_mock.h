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

#pragma once

#include "common/platform.h"

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
