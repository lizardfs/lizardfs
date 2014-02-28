#pragma once

#include <string>
#include <vector>

#include "common/exception.h"
#include "common/packet.h"
#include "common/sockets.h"

LIZARDFS_CREATE_EXCEPTION_CLASS(WrongUsageException, Exception);

class LizardFsProbeCommand {
public:
	static const std::string kPorcelainMode;
	static const std::string kVerboseMode;

	virtual ~LizardFsProbeCommand() {}
	virtual std::string name() const = 0;
	virtual void usage() const = 0;
	virtual void run(const std::vector<std::string>& argv) const = 0;

protected:
	static std::vector<uint8_t> askMaster(const std::vector<uint8_t>& request,
			const std::string& masterHost, const std::string& masterPort,
			PacketHeader::Type responseType);
	static int connect(const std::string& host, const std::string& port);
	static std::vector<uint8_t> sendAndReceive(int fd,
			const std::vector<uint8_t>& request,
			PacketHeader::Type expectedType);
};
