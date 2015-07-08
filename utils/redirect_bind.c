#define _GNU_SOURCE
#include <dlfcn.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>

int bind(int sockfd, const struct sockaddr *addr, socklen_t addrlen) {
	static int (*_bind)(int, const struct sockaddr*, socklen_t) = NULL;
	struct sockaddr_in addr_in = * (const struct sockaddr_in*) addr;
	int type;
	socklen_t length = sizeof(type);
	getsockopt(sockfd, SOL_SOCKET, SO_TYPE, &type, &length);
	if (type == SOCK_STREAM) {
		int port = ntohs(addr_in.sin_port);
		if (port) {
			port += 1000;
			addr_in.sin_port = htons(port);
		}
	}
	if (!_bind) {
		_bind = dlsym(RTLD_NEXT, "bind");
	}
	return _bind(sockfd, (const struct sockaddr*) &addr_in, addrlen);
}

