/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2015
   Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"
#include "common/sockets.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#ifdef _WIN32
#include <ws2tcpip.h>
#include <winsock2.h>
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#endif

/* Acid's simple socket library - ver 2.0 */

/* ---------------SOCK INIT--------------- */

int socketinit() {
#ifdef _WIN32
	WORD version_requested;
	WSADATA wsa_data;

	version_requested = MAKEWORD(2, 2);
	return WSAStartup(version_requested, &wsa_data);
#else
	return 0;
#endif
}

int socketrelease() {
#ifdef _WIN32
	return WSACleanup();
#else
	return 0;
#endif
}

/* ---------------SOCK ADDR--------------- */

static inline int sockaddrnumfill(struct sockaddr_in *sa, uint32_t ip, uint16_t port) {
	memset(sa, 0, sizeof(struct sockaddr_in));
	sa->sin_family = AF_INET;
	sa->sin_port = htons(port);
	sa->sin_addr.s_addr = htonl(ip);
	return 0;
}

static inline int sockaddrfill(struct sockaddr_in *sa, const char *hostname, const char *service,
				int family, int socktype, int passive) {
	struct addrinfo hints, *res, *reshead;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = family;
	hints.ai_socktype = socktype;
	if (passive) {
		hints.ai_flags = AI_PASSIVE;
	}
	if (hostname && hostname[0] == '*') {
		hostname = NULL;
	}
	if (service && service[0] == '*') {
		service = NULL;
	}
	if (getaddrinfo(hostname, service, &hints, &reshead)) {
		return -1;
	}
	for (res = reshead; res; res = res->ai_next) {
		if (res->ai_family == family && res->ai_socktype == socktype &&
		    res->ai_addrlen == sizeof(struct sockaddr_in)) {
			*sa = *((struct sockaddr_in *)(res->ai_addr));
			freeaddrinfo(reshead);
			return 0;
		}
	}
	freeaddrinfo(reshead);
	return -1;
}

static inline int sockresolve(const char *hostname, const char *service, uint32_t *ip,
				uint16_t *port, int family, int socktype, int passive) {
	struct sockaddr_in sa;
	if (sockaddrfill(&sa, hostname, service, family, socktype, passive) < 0) {
		return -1;
	}
	if (ip != (void *)0) {
		*ip = ntohl(sa.sin_addr.s_addr);
	}
	if (port != (void *)0) {
		*port = ntohs(sa.sin_port);
	}
	return 0;
}

static inline int socknonblock(int sock) {
#ifdef _WIN32
	u_long yes = 1;
	return ioctlsocket(sock, FIONBIO, &yes);
#else
#ifdef O_NONBLOCK
	int flags = fcntl(sock, F_GETFL, 0);
	if (flags == -1) {
		return -1;
	}
	return fcntl(sock, F_SETFL, flags | O_NONBLOCK);
#else
	int yes = 1;
	return ioctl(sock, FIONBIO, &yes);
#endif
#endif
}

/* ----------------- TCP ----------------- */

int tcpgetlasterror() {
#ifdef _WIN32
	return WSAGetLastError();
#else
	return errno;
#endif
}

void tcpsetlasterror(int err) {
#ifdef _WIN32
	WSASetLastError(err);
#else
	errno = err;
#endif
}

int tcpsetacceptfilter(int sock) {
#ifdef SO_ACCEPTFILTER
	struct accept_filter_arg afa;

	memset(&ata, 0, sizeof(afa));
	strcpy(afa.af_name, "dataready");
	return setsockopt(sock, SOL_SOCKET, SO_ACCEPTFILTER, &afa, sizeof(afa));
#elif TCP_DEFER_ACCEPT
	int v = 1;

	return setsockopt(sock, IPPROTO_TCP, TCP_DEFER_ACCEPT, &v, sizeof(v));
#else
	(void)sock;
	tcpsetlasterror(TCPENOTSUP);
	return -1;
#endif
}

int tcpsocket(void) {
	return socket(AF_INET, SOCK_STREAM, 0);
}

int tcpnonblock(int sock) {
	return socknonblock(sock);
}

int tcpresolve(const char *hostname, const char *service, uint32_t *ip, uint16_t *port,
		int passive) {
	return sockresolve(hostname, service, ip, port, AF_INET, SOCK_STREAM, passive);
}

int tcpreuseaddr(int sock) {
	int yes = 1;
	return setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (char *)&yes, sizeof(int));
}

int tcpnodelay(int sock) {
	int yes = 1;
	return setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char *)&yes, sizeof(int));
}

int tcpaccfhttp(int sock) {
#ifdef SO_ACCEPTFILTER
	struct accept_filter_arg afa;

	memset(&afa, 0, sizeof(afa));
	strcpy(afa.af_name, "httpready");
	return setsockopt(sock, SOL_SOCKET, SO_ACCEPTFILTER, &afa, sizeof(afa));
#else
	(void)sock;
	tcpsetlasterror(TCPEINVAL);
	return -1;
#endif
}

int tcpaccfdata(int sock) {
#ifdef SO_ACCEPTFILTER
	struct accept_filter_arg afa;

	memset(&afa, 0, sizeof(afa));
	strcpy(afa.af_name, "dataready");
	return setsockopt(sock, SOL_SOCKET, SO_ACCEPTFILTER, &afa, sizeof(afa));
#else
	(void)sock;
	tcpsetlasterror(TCPEINVAL);
	return -1;
#endif
}

int tcpstrbind(int sock, const char *hostname, const char *service) {
	struct sockaddr_in sa;
	if (sockaddrfill(&sa, hostname, service, AF_INET, SOCK_STREAM, 1) < 0) {
		return -1;
	}
	if (bind(sock, (struct sockaddr *)&sa, sizeof(struct sockaddr_in)) < 0) {
		return -1;
	}
	return 0;
}

int tcpnumbind(int sock, uint32_t ip, uint16_t port) {
	struct sockaddr_in sa;
	sockaddrnumfill(&sa, ip, port);
	if (bind(sock, (struct sockaddr *)&sa, sizeof(struct sockaddr_in)) < 0) {
		return -1;
	}
	return 0;
}

int tcpstrconnect(int sock, const char *hostname, const char *service) {
	struct sockaddr_in sa;
	if (sockaddrfill(&sa, hostname, service, AF_INET, SOCK_STREAM, 0) < 0) {
		return -1;
	}
	if (connect(sock, (struct sockaddr *)&sa, sizeof(struct sockaddr_in)) >= 0) {
		return 0;
	}
	int err = tcpgetlasterror();
	if (err == TCPEINPROGRESS) {
		return 1;
	}
	return -1;
}

int tcpnumconnect(int sock, uint32_t ip, uint16_t port) {
	struct sockaddr_in sa;
	sockaddrnumfill(&sa, ip, port);
	if (connect(sock, (struct sockaddr *)&sa, sizeof(struct sockaddr_in)) >= 0) {
		return 0;
	}
	int err = tcpgetlasterror();
	if (err == TCPEINPROGRESS) {
		return 1;
	}
	return -1;
}

int tcpstrtoconnect(int sock, const char *hostname, const char *service, uint32_t msecto) {
	struct sockaddr_in sa;
	if (socknonblock(sock) < 0) {
		return -1;
	}
	if (sockaddrfill(&sa, hostname, service, AF_INET, SOCK_STREAM, 0) < 0) {
		return -1;
	}
	if (connect(sock, (struct sockaddr *)&sa, sizeof(struct sockaddr_in)) >= 0) {
		return 0;
	}
	int err = tcpgetlasterror();
	if (err == TCPEINPROGRESS) {
		struct pollfd pfd;
		pfd.fd = sock;
		pfd.events = POLLOUT;
		pfd.revents = 0;
		if (tcppoll(pfd, msecto) < 0) {
			return -1;
		}
		if (pfd.revents & POLLOUT) {
			return tcpgetstatus(sock);
		}
		tcpsetlasterror(TCPETIMEDOUT);
	}
	return -1;
}

int tcpnumtoconnect(int sock, uint32_t ip, uint16_t port, uint32_t msecto) {
	struct sockaddr_in sa;
	if (socknonblock(sock) < 0) {
		return -1;
	}
	sockaddrnumfill(&sa, ip, port);
	if (connect(sock, (struct sockaddr *)&sa, sizeof(struct sockaddr_in)) >= 0) {
		return 0;
	}
	int err = tcpgetlasterror();
	if (err == TCPEINPROGRESS) {
		struct pollfd pfd;
		pfd.fd = sock;
		pfd.events = POLLOUT;
		pfd.revents = 0;
		if (tcppoll(pfd, msecto) < 0) {
			return -1;
		}
		if (pfd.revents & POLLOUT) {
			return tcpgetstatus(sock);
		}
		tcpsetlasterror(TCPETIMEDOUT);
	}
	return -1;
}

int tcpgetstatus(int sock) {
	socklen_t arglen = sizeof(int);
	int rc = 0;
#ifdef _WIN32
	if (getsockopt(sock, SOL_SOCKET, SO_ERROR, (char *)&rc, &arglen) < 0) {
#else
	if (getsockopt(sock, SOL_SOCKET, SO_ERROR, (void *)&rc, &arglen) < 0) {
#endif
		rc = tcpgetlasterror();
	}
	tcpsetlasterror(rc);
	return rc;
}

int tcpstrlisten(int sock, const char *hostname, const char *service, uint16_t queue) {
	struct sockaddr_in sa;
	if (sockaddrfill(&sa, hostname, service, AF_INET, SOCK_STREAM, 1) < 0) {
		return -1;
	}
	if (bind(sock, (struct sockaddr *)&sa, sizeof(struct sockaddr_in)) < 0) {
		return -1;
	}
	if (listen(sock, queue) < 0) {
		return -1;
	}
	return 0;
}

int tcpnumlisten(int sock, uint32_t ip, uint16_t port, uint16_t queue) {
	struct sockaddr_in sa;
	sockaddrnumfill(&sa, ip, port);
	if (bind(sock, (struct sockaddr *)&sa, sizeof(struct sockaddr_in)) < 0) {
		return -1;
	}
	if (listen(sock, queue) < 0) {
		return -1;
	}
	return 0;
}

int tcpaccept(int lsock) {
	int sock;
	sock = accept(lsock, (struct sockaddr *)NULL, 0);
	if (sock < 0) {
		return -1;
	}
	return sock;
}

int tcpgetpeer(int sock, uint32_t *ip, uint16_t *port) {
	struct sockaddr_in sa;
	socklen_t leng;
	leng = sizeof(sa);
	if (getpeername(sock, (struct sockaddr *)&sa, &leng) < 0) {
		return -1;
	}
	if (ip != (void *)0) {
		*ip = ntohl(sa.sin_addr.s_addr);
	}
	if (port != (void *)0) {
		*port = ntohs(sa.sin_port);
	}
	return 0;
}

int tcpgetmyaddr(int sock, uint32_t *ip, uint16_t *port) {
	struct sockaddr_in sa;
	socklen_t leng;
	leng = sizeof(sa);
	if (getsockname(sock, (struct sockaddr *)&sa, &leng) < 0) {
		return -1;
	}
	if (ip != (void *)0) {
		*ip = ntohl(sa.sin_addr.s_addr);
	}
	if (port != (void *)0) {
		*port = ntohs(sa.sin_port);
	}
	return 0;
}

int tcpclose(int sock) {
// make sure that all pending data in the output buffer will be sent
#ifdef _WIN32
	shutdown(sock, SD_SEND);
	return closesocket(sock);
#else
	shutdown(sock, SHUT_WR);
	return close(sock);
#endif
}

#if defined(SOCKET_CONVERT_POLL_TO_SELECT)

int tcppoll(pollfd &pfd, int msecto) {
	fd_set read_set, write_set, except_set;
	fd_set *read_set_ptr = NULL, *write_set_ptr = NULL, *except_set_ptr = NULL;

	FD_ZERO(&read_set);
	FD_ZERO(&write_set);
	FD_ZERO(&except_set);

	if (pfd.events & (POLLRDNORM | POLLIN)) {
		read_set_ptr = &read_set;
		FD_SET(pfd.fd, &read_set);
	}
	if (pfd.events & (POLLWRNORM | POLLOUT)) {
		write_set_ptr = &write_set;
		FD_SET(pfd.fd, &write_set);
	}
	if (pfd.events & (POLLRDBAND | POLLPRI)) {
		except_set_ptr = &except_set;
		FD_SET(pfd.fd, &except_set);
	}

#ifdef _WIN32
	// Winsock can't handle empty fd_sets
	if (!read_set_ptr && !write_set_ptr && !except_set_ptr) {
		if (msecto < 0) {
			tcpsetlasterror(TCPEINVAL);
			return -1;
		}
		Sleep(msecto);
		return 0;
	}
#endif

	timeval tv;
	timeval *tv_ptr = NULL;
	if (msecto >= 0) {
		tv_ptr = &tv;
		tv.tv_sec = msecto / 1000;
		tv.tv_usec = 1000 * (msecto % 1000);
	}

	int ret = select(pfd.fd + 1, read_set_ptr, write_set_ptr, except_set_ptr, tv_ptr);

	if (ret == -1) {
		return ret;
	}

	pfd.revents = 0;

	if (FD_ISSET(pfd.fd, &read_set)) {
		pfd.revents |= POLLIN;
	}
	if (FD_ISSET(pfd.fd, &write_set)) {
		pfd.revents |= POLLOUT;
	}
	if (FD_ISSET(pfd.fd, &except_set)) {
		pfd.revents |= POLLPRI;
	}

	return ret;
}

int tcppoll(std::vector<pollfd> &pfd, int msecto) {
	fd_set read_set, write_set, except_set;
	fd_set *read_set_ptr = NULL, *write_set_ptr = NULL, *except_set_ptr = NULL;
	int max_fd = 0;

	FD_ZERO(&read_set);
	FD_ZERO(&write_set);
	FD_ZERO(&except_set);

	for (const auto &p : pfd) {
		if (p.events & (POLLRDNORM | POLLIN)) {
			read_set_ptr = &read_set;
			FD_SET(p.fd, &read_set);
		}
		if (p.events & (POLLWRNORM | POLLOUT)) {
			write_set_ptr = &write_set;
			FD_SET(p.fd, &write_set);
		}
		if (p.events & (POLLRDBAND | POLLPRI)) {
			except_set_ptr = &except_set;
			FD_SET(p.fd, &except_set);
		}
		max_fd = std::max(max_fd, (int)p.fd);
	}

#ifdef _WIN32
	// Winsock can't handle empty fd_sets
	if (!read_set_ptr && !write_set_ptr && !except_set_ptr) {
		if (msecto < 0) {
			tcpsetlasterror(TCPEINVAL);
			return -1;
		}
		Sleep(msecto);
		return 0;
	}
#endif

	timeval tv;
	timeval *tv_ptr = NULL;
	if (msecto >= 0) {
		tv_ptr = &tv;
		tv.tv_sec = msecto / 1000;
		tv.tv_usec = 1000 * (msecto % 1000);
	}
	int ret = select(max_fd + 1, read_set_ptr, write_set_ptr, except_set_ptr, tv_ptr);

	if (ret == -1) {
		return -1;
	}

	ret = 0;
	for (auto &p : pfd) {
		p.revents = 0;

		if (FD_ISSET(p.fd, &read_set)) {
			p.revents |= POLLIN;
		}
		if (FD_ISSET(p.fd, &write_set)) {
			p.revents |= POLLOUT;
		}
		if (FD_ISSET(p.fd, &except_set)) {
			p.revents |= POLLPRI;
		}
		ret += p.revents != 0;
	}

	return ret;
}

#else

int tcppoll(pollfd &pfd, int msecto) {
#ifdef _WIN32
	return WSAPoll(&pfd, 1, msecto);
#else
	return poll(&pfd, 1, msecto);
#endif
}

int tcppoll(std::vector<pollfd> &pfd, int msecto) {
#ifdef _WIN32
	return WSAPoll(pfd.data(), pfd.size(), msecto);
#else
	return poll(pfd.data(), pfd.size(), msecto);
#endif
}

#endif

int tcptopoll(int sock, int events, int msecto) {
	struct pollfd pfd;
	pfd.fd = sock;
	pfd.events = events;
	pfd.revents = 0;
	return tcppoll(pfd, msecto);
}

int32_t tcprecv(int sock, void *buff, uint32_t len, int flags) {
#ifdef _WIN32
	return recv(sock, (char *)buff, len, flags);
#else
	return recv(sock, buff, len, flags);
#endif
}

int32_t tcpsend(int sock, const void *buff, uint32_t len, int flags) {
#ifdef _WIN32
	return send(sock, (const char *)buff, len, flags);
#else
	return send(sock, buff, len, flags);
#endif
}

int32_t tcptoread(int sock, void *buff, uint32_t leng, uint32_t msecto) {
	uint32_t rcvd = 0;
	int i;
	struct pollfd pfd;
	pfd.fd = sock;
	pfd.events = POLLIN;
	while (rcvd < leng) {
		pfd.revents = 0;
		if (tcppoll(pfd, msecto) < 0) {
			return -1;
		}
		if (pfd.revents & POLLIN) {
			i = tcprecv(sock, ((uint8_t *)buff) + rcvd, leng - rcvd, 0);
			if (i == 0 || (i < 0 && tcpgetlasterror() != TCPEAGAIN)) {
				return i;
			} else if (i > 0) {
				rcvd += i;
			}
		} else {
			tcpsetlasterror(TCPETIMEDOUT);
			return -1;
		}
	}
	return rcvd;
}

int32_t tcptowrite(int sock, const void *buff, uint32_t leng, uint32_t msecto) {
	uint32_t sent = 0;
	int i;
	struct pollfd pfd;
	pfd.fd = sock;
	pfd.events = POLLOUT;
	while (sent < leng) {
		pfd.revents = 0;
		if (tcppoll(pfd, msecto) < 0) {
			return -1;
		}
		if (pfd.revents & POLLOUT) {
			i = tcpsend(sock, ((uint8_t *)buff) + sent, leng - sent, 0);
			if (i == 0 || (i < 0 && tcpgetlasterror() != TCPEAGAIN)) {
				return i;
			} else if (i > 0) {
				sent += i;
			}
		} else {
			tcpsetlasterror(TCPETIMEDOUT);
			return -1;
		}
	}
	return sent;
}

int tcptoaccept(int sock, uint32_t msecto) {
	struct pollfd pfd;
	pfd.fd = sock;
	pfd.events = POLLIN;
	pfd.revents = 0;
	if (tcppoll(pfd, msecto) < 0) {
		return -1;
	}
	if (pfd.revents & POLLIN) {
		return accept(sock, (struct sockaddr *)NULL, 0);
	}
	tcpsetlasterror(TCPETIMEDOUT);
	return -1;
}

/* ----------------- UDP ----------------- */

int udpsocket(void) {
	return socket(AF_INET, SOCK_DGRAM, 0);
}

int udpnonblock(int sock) {
	return socknonblock(sock);
}

int udpresolve(const char *hostname, const char *service, uint32_t *ip, uint16_t *port,
		int passive) {
	return sockresolve(hostname, service, ip, port, AF_INET, SOCK_DGRAM, passive);
}

int udpnumlisten(int sock, uint32_t ip, uint16_t port) {
	struct sockaddr_in sa;
	sockaddrnumfill(&sa, ip, port);
	return bind(sock, (struct sockaddr *)&sa, sizeof(struct sockaddr_in));
}

int udpstrlisten(int sock, const char *hostname, const char *service) {
	struct sockaddr_in sa;
	if (sockaddrfill(&sa, hostname, service, AF_INET, SOCK_DGRAM, 1) < 0) {
		return -1;
	}
	return bind(sock, (struct sockaddr *)&sa, sizeof(struct sockaddr_in));
}

int udpwrite(int sock, uint32_t ip, uint16_t port, const void *buff, uint16_t leng) {
	struct sockaddr_in sa;
	if (leng > 512) {
		return -1;
	}
	sockaddrnumfill(&sa, ip, port);
#ifdef _WIN32
	return sendto(sock, (const char *)buff, leng, 0, (struct sockaddr *)&sa,
	              sizeof(struct sockaddr_in));
#else
	return sendto(sock, buff, leng, 0, (struct sockaddr *)&sa, sizeof(struct sockaddr_in));
#endif
}

int udpread(int sock, uint32_t *ip, uint16_t *port, void *buff, uint16_t leng) {
	socklen_t templeng;
	struct sockaddr tempaddr;
	struct sockaddr_in *saptr;
	int ret;
#ifdef _WIN32
	ret = recvfrom(sock, (char *)buff, leng, 0, &tempaddr, &templeng);
#else
	ret = recvfrom(sock, buff, leng, 0, &tempaddr, &templeng);
#endif
	if (templeng == sizeof(struct sockaddr_in)) {
		saptr = ((struct sockaddr_in *)&tempaddr);
		if (ip != (void *)0) {
			*ip = ntohl(saptr->sin_addr.s_addr);
		}
		if (port != (void *)0) {
			*port = ntohs(saptr->sin_port);
		}
	}
	return ret;
}

int udpclose(int sock) {
#ifdef _WIN32
	return closesocket(sock);
#else
	return close(sock);
#endif
}
