#include <iostream>

#include "common/exception.h"
#include "common/MFSCommunication.h"
#include "common/message_receive_buffer.h"
#include "common/multi_buffer_writer.h"
#include "common/packet.h"
#include "common/sockets.h"

LIZARDFS_CREATE_EXCEPTION_CLASS(WrongUsageException, Exception);

int connect(const std::string& host, const std::string& port);
std::vector<uint8_t> sendAndReceive(int fd,
		const std::vector<uint8_t>& request,
		PacketHeader::Type expectedType);

class LizardFsProbeCommand {
public:
	virtual ~LizardFsProbeCommand() {}
	virtual std::string name() const = 0;
	virtual void usage() const = 0;
	virtual int run(const std::vector<std::string>& argv) const = 0;
};

class ReadyChunkservers : public LizardFsProbeCommand {
public:
	virtual std::string name() const {
		return "ready-chunkservers";
	}

	virtual void usage() const {
		std::cerr << name() << " <master ip> <master port>" << std::endl;
		std::cerr << "    prints number of chunkservers ready to be written to" << std::endl;
	}

	virtual int run(const std::vector<std::string>& argv) const {
		if (argv.size() != 2) {
			throw WrongUsageException("Expected exactly two arguments for " + name());
		}

		std::vector<uint8_t> request, response;
		serializeMooseFsPacket(request, CLTOMA_CSERV_LIST);
		int fd = connect(argv[0], argv[1]);
		try {
			response = sendAndReceive(fd, request, MATOCL_CSERV_LIST);
			tcpclose(fd);
		} catch (...) {
			tcpclose(fd);
			throw;
		}

		uint32_t version, ip, chunks, tdChunks, errorCount;
		uint64_t usedSpace, totalSpace, tdUsedSpace, tdTotalSpace;
		uint16_t port;
		uint32_t readyChunkservers = 0;
		uint32_t entrySize = serializedSize(version, ip, port, usedSpace, totalSpace, chunks,
				tdUsedSpace, tdTotalSpace, tdChunks, errorCount);
		while (!response.empty()) {
			deserialize(response, version, ip, port, usedSpace, totalSpace, chunks,
					tdUsedSpace, tdTotalSpace, tdChunks, errorCount);
			if (totalSpace > 0) {
				readyChunkservers++;
			}
			response.erase(response.begin(), response.begin() + entrySize);
		}
		std::cout << readyChunkservers << std::endl;
		return 0;
	}
};

int connect(const std::string& host, const std::string& port) {
	int fd = tcpsocket();
	if (fd < 0) {
		throw Exception("Can't create socket");
	}
	if (tcpstrconnect(fd, host.c_str(), port.c_str()) != 0) {
		tcpclose(fd);
		throw Exception("Can't connect to " + host + ":" + port);
	}
	return fd;
}

std::vector<uint8_t> sendAndReceive(int fd,
		const std::vector<uint8_t>& request,
		PacketHeader::Type expectedType) {
	// Send
	MultiBufferWriter writer;
	writer.addBufferToSend(request.data(), request.size());
	while (writer.hasDataToSend()) {
		ssize_t bytesWritten = writer.writeTo(fd);
		if (bytesWritten < 0) {
			throw Exception("Can't write data to socket");
		}
	}

	// Receive
	MessageReceiveBuffer reader(4 * 1024 * 1024);
	while (!reader.hasMessageData()) {
		ssize_t bytesRead = reader.readFrom(fd);
		if (bytesRead < 0) {
			throw Exception("Can't read data from socket");
		}
		if (reader.isMessageTooBig()) {
			throw Exception("Receive buffer overflow");
		}
	}
	if (reader.getMessageHeader().type != expectedType) {
		throw Exception("Received unexpected message");
	}

	uint32_t length = reader.getMessageHeader().length;
	return std::vector<uint8_t>(reader.getMessageData(), reader.getMessageData() + length);
}

int main(int argc, const char** argv) {
	std::vector<const LizardFsProbeCommand*> allCommands = {
			new ReadyChunkservers(),
	};

	try {
		if (argc < 2) {
			throw WrongUsageException("No command name provided");
		}
		std::string commandName(argv[1]);
		std::vector<std::string> arguments(argv + 2, argv + argc);
		for (auto command : allCommands) {
			if (command->name() == commandName) {
				return command->run(arguments);
			}
		}
		throw WrongUsageException("Unknown command " + commandName);
	} catch (WrongUsageException& ex) {
		std::cerr << ex.message() << std::endl;
		std::cerr << "Usage:\n";
		std::cerr << "    " << argv[0] << " COMMAND [ARGUMENTS...]\n\n";
		std::cerr << "Available COMMANDs:\n\n";
		for (auto command : allCommands) {
			command->usage();
			std::cerr << std::endl;
		}
		return 1;
	} catch (Exception& ex) {
		std::cerr << "Error: " << ex.what() << std::endl;
		return 1;
	}
}
