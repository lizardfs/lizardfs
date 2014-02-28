#include "lizardfs_probe_command.h"

#include "common/message_receive_buffer.h"
#include "common/multi_buffer_writer.h"

const std::string LizardFsProbeCommand::kPorcelainMode = "--porcelain";
const std::string LizardFsProbeCommand::kVerboseMode = "--verbose";

int LizardFsProbeCommand::connect(const std::string& host, const std::string& port) {
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

std::vector<uint8_t> LizardFsProbeCommand::askMaster(const std::vector<uint8_t>& request,
		const std::string& masterHost, const std::string& masterPort,
		PacketHeader::Type responseType) {
	std::vector<uint8_t> response;
	int fd = connect(masterHost, masterPort);
	try {
		response = sendAndReceive(fd, request, responseType);
		tcpclose(fd);
	} catch (...) {
		tcpclose(fd);
		throw;
	}
	return response;
}

std::vector<uint8_t> LizardFsProbeCommand::sendAndReceive(int fd,
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
