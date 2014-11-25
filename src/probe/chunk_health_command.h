#pragma once

#include "common/platform.h"

#include <map>

#include "common/chunks_availability_state.h"
#include "common/server_connection.h"
#include "probe/lizardfs_probe_command.h"

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
