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

#include <sys/time.h>
#include <sys/socket.h>
#include <iostream>
#include <string>

#include "protocol/MFSCommunication.h"
#include "protocol/packet.h"
#include "common/serialization.h"
#include "common/sockets.h"


int main(int argc, char **argv) {
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
