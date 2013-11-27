#include <sys/time.h>
#include <sys/socket.h>
#include <iostream>
#include <string>

#include "common/MFSCommunication.h"
#include "common/packet.h"
#include "common/serialization.h"
#include "common/sockets.h"


int main(int argc, char **argv) {
	strerr_init();
	if (argc != 2) {
		std::cerr << "Usage: " << argv[0] << " port" << std::endl;
		return 1;
	}

	int fd = tcpsocket();
	eassert(fd >= 0);
	tcpnodelay(fd);
	tcpreuseaddr(fd);
	eassert(tcpstrlisten(fd, "*", argv[1], 100) == 0);

	int client = -1;
	std::vector<uint8_t> request(12);
	std::vector<uint8_t> reply;
	while((client = accept(fd, NULL, 0)) != -1) {
		std::cerr << "Accepted" << std::endl;
		while (true) {
			ssize_t ret = tcptoread(client, request.data(), request.size(), 10000);
			if (ret != (ssize_t)request.size()) {
				break;
			}
			uint32_t type;
			uint32_t length;
			uint32_t size;
			deserialize(request, type, length, size);
			sassert(type == ANTOAN_PING && length == 4);
			sassert(size < 2000000);
			reply.clear();
			serialize(reply, uint32_t(ANTOAN_PING_REPLY), uint32_t(size));
			reply.resize(reply.size() + size);
			ret = tcptowrite(client, reply.data(), reply.size(), 10000);
			if (ret != (ssize_t)(reply.size())) {
				break;
			}
		}
		tcpclose(client);
		client = -1;
	}
	return 0;
}
