/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

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

#include "common/chunks_availability_state.h"
#include "common/server_connection.h"
#include "admin/lizardfs_admin_command.h"

class ChunksHealthCommand : public LizardFsProbeCommand {
public:
	virtual std::string name() const;
	virtual SupportedOptions supportedOptions() const;
	virtual void usage() const;
	virtual void run(const Options& options) const;

private:
	static const std::string kOptionAll;
	static const std::string kOptionAvailability;
	static const std::string kOptionReplication;
	static const std::string kOptionDeletion;

	static void initializeGoals(ServerConnection& connection);

	void printState(const ChunksAvailabilityState& state, bool isPorcelain) const;
	void printState(bool isReplication, const ChunksReplicationState& state,
			bool isPorcelain) const;
	std::string print(uint64_t number) const;

	static std::vector<uint8_t> goals;
	static std::map<uint8_t, std::string> goalNames;
};
