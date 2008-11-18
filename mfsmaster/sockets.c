/*
   Copyright 2008 Gemius SA.

   This file is part of MooseFS.

   MooseFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   MooseFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with MooseFS.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "config.h"

#include <sys/types.h>
#ifndef WIN32
#include <sys/socket.h>
#include <sys/poll.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <inttypes.h>
#else
#include <winsock2.h>
#define EINPROGRESS WSAEINPROGRESS
#endif
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#ifdef _THREAD_SAFE
#include <pthread.h>

static pthread_mutex_t hmutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t amutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t gmutex = PTHREAD_MUTEX_INITIALIZER;
#endif

#ifndef INADDR_NONE
#define INADDR_NONE (in_addr_t)(-1)
#endif

#include "sockets.h"
/* Acid's simple socket library - ver 1.9 */

// amutex:
static struct sockaddr_in *addrtab=NULL;
static uint32_t addrlen = 0;
static uint32_t addrmax = 0;

// gmutex:
static int32_t globaladdr = -1;

/* ---------------SOCK ADDR--------------- */

int32_t sockaddrnewempty() {
	uint32_t r;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&amutex);
#endif
	if (addrlen == addrmax) {
		if (addrmax==0) {
			addrmax=50;
		} else {
			addrmax*=2;
		}
		addrtab = (struct sockaddr_in *)realloc((void*)addrtab,addrmax*sizeof(struct sockaddr_in));
		if (addrtab==NULL) {
#ifdef _THREAD_SAFE
			pthread_mutex_unlock(&amutex);
#endif
			return -1;
		}
	}
	r = addrlen++;
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&amutex);
#endif
	return r;
}

int sockaddrnumchange(uint32_t i,uint32_t ip,uint16_t port) {
	if (i>=addrlen) return -1;
	memset((char *)(addrtab+i),0,sizeof(struct sockaddr_in));
	addrtab[i].sin_family = AF_INET;
	addrtab[i].sin_port = htons(port);
	addrtab[i].sin_addr.s_addr = htonl(ip);
	return 0;
}

int sockaddrchange(uint32_t i,const char *hostname,const char *service,const char *proto) {
	uint16_t port;
	struct in_addr addr;
	char *endp;
	uint32_t temp;

	if (!hostname || !service || !proto || i>=addrlen) return -1;

#ifdef _THREAD_SAFE
	pthread_mutex_lock(&hmutex);
#endif
	port=0;
	if (service[0]!='*') {
		temp = strtol(service, &endp, 0);
		if (*endp == '\0' && temp > 0 && temp < 65536) {
			port = htons((uint16_t) temp);
		} else {
			struct servent *serv = getservbyname(service, proto);
			if (serv) port = serv->s_port;
		}
	}

	addr.s_addr = INADDR_ANY;
	if (hostname[0]!='*') {
		temp = (uint32_t)inet_addr(hostname);
		if (temp==INADDR_NONE) {
			struct hostent *host = gethostbyname(hostname);
			if (host) addr = *((struct in_addr *)(host->h_addr_list[0]));
		} else {
			addr.s_addr = temp;
		}
	}
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&hmutex);
#endif

	memset((char *)(addrtab+i),0,sizeof(struct sockaddr_in));
	addrtab[i].sin_family = AF_INET;
	addrtab[i].sin_port = port;
	addrtab[i].sin_addr = addr;
	return 0;
}

int sockaddrnew(const char *hostname,const char *service,const char *proto) {
	uint32_t i;
	i = sockaddrnewempty();
	if (sockaddrchange(i,hostname,service,proto)<0) return -1;
	return i;
}


int sockaddrget(uint32_t i,uint32_t *ip,uint16_t *port) {
	if (i>=addrlen) return -1;
	if (ip!=(void *)0) {
		*ip = ntohl(addrtab[i].sin_addr.s_addr);
	}
	if (port!=(void *)0) {
		*port = ntohs(addrtab[i].sin_port);
	}
	return 0;
}

int sockaddrconvert(const char *hostname,const char *service,const char *proto,uint32_t *ip,uint16_t *port) {
	int r;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&gmutex);
#endif
	if (globaladdr<0) globaladdr = sockaddrnewempty();
	if (sockaddrchange(globaladdr,hostname,service,proto)<0) {
#ifdef _THREAD_SAFE
		pthread_mutex_unlock(&gmutex);
#endif
		return -1;
	}
	r = sockaddrget(globaladdr,ip,port);
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&gmutex);
#endif
	return r;
}

#ifndef WIN32
int socknonblock(int sock) {
#ifdef O_NONBLOCK
	int flags = fcntl(sock, F_GETFL, 0);
	if (flags == -1) return -1;
	return fcntl(sock, F_SETFL, flags | O_NONBLOCK);
#else
	int yes = 1;
	return ioctl(sock, FIONBIO, &yes);
#endif
}
#endif /* WIN32 */

/* ----------------- TCP ----------------- */

int tcpsocket(void) {
	return socket(AF_INET,SOCK_STREAM,0);
}

int tcpreuseaddr(int sock) {
	int yes=1;
	return setsockopt(sock,SOL_SOCKET,SO_REUSEADDR,(char*)&yes,sizeof(int));
}

int tcpnodelay(int sock) {
	int yes=1;
	return setsockopt(sock,IPPROTO_TCP,TCP_NODELAY,(char*)&yes,sizeof(int));
}

int tcpaccfhttp(int sock) {
#ifdef SO_ACCEPTFILTER
	struct accept_filter_arg afa;

	bzero(&afa, sizeof(afa));
	strcpy(afa.af_name, "httpready");
	return setsockopt(sock, SOL_SOCKET, SO_ACCEPTFILTER, &afa, sizeof(afa));
#else
	(void)sock;
	errno=EINVAL;
	return -1;
#endif
}

int tcpaccfdata(int sock) {
#ifdef SO_ACCEPTFILTER
	struct accept_filter_arg afa;

	bzero(&afa, sizeof(afa));
	strcpy(afa.af_name, "dataready");
	return setsockopt(sock, SOL_SOCKET, SO_ACCEPTFILTER, &afa, sizeof(afa));
#else
	(void)sock;
	errno=EINVAL;
	return -1;
#endif
}

int tcpconnect(int sock,const char *hostname,const char *service) {
	int rc;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&gmutex);
#endif
	if (globaladdr<0) globaladdr = sockaddrnewempty();
	sockaddrchange(globaladdr,hostname,service,"tcp");
	rc = connect(sock, (struct sockaddr *)(addrtab+globaladdr),sizeof(struct sockaddr_in));
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&gmutex);
#endif
	if (rc >= 0) return 0;
	if (errno == EINPROGRESS) return 1;
	return -1;
}

int tcpnumconnect(int sock,uint32_t ip,uint16_t port) {
	int rc;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&gmutex);
#endif
	if (globaladdr<0) globaladdr = sockaddrnewempty();
	sockaddrnumchange(globaladdr,ip,port);
	rc = connect(sock, (struct sockaddr *)(addrtab+globaladdr),sizeof(struct sockaddr_in));
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&gmutex);
#endif
	if (rc >= 0) return 0;
	if (errno == EINPROGRESS) return 1;
	return -1;
}

int tcpaddrconnect(int sock,uint32_t addr) {
	int rc;

	if (addr>addrlen) return -1;
	rc = connect(sock, (struct sockaddr *)(addrtab+addr),sizeof(struct sockaddr_in));
	if (rc >= 0) return 0;
	if (errno == EINPROGRESS) return 1;
	return -1;
}

int tcpgetstatus(int sock) {
	socklen_t arglen = sizeof(int);
	int rc = 0;
#ifndef WIN32
	if (getsockopt(sock,SOL_SOCKET,SO_ERROR,(void *)&rc,&arglen) < 0) rc=errno;
#else
	if (getsockopt(sock,SOL_SOCKET,SO_ERROR,(char *)&rc,&arglen) < 0) rc=errno;
#endif
	errno=rc;
	return rc;
}

int tcpnumlisten(int sock,uint32_t ip,uint16_t port,uint16_t queue) {
	int r;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&gmutex);
#endif
	if (globaladdr<0) globaladdr = sockaddrnewempty();
	sockaddrnumchange(globaladdr,ip,port);
	r = bind(sock, (struct sockaddr *)(addrtab+globaladdr),sizeof(struct sockaddr_in));
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&gmutex);
#endif
	if (r<0) {
		return -1;
	}
	if (listen(sock,queue)<0) {
		return -1;
	}
	return 0;
}

int tcplisten(int sock,const char *hostname,const char *service,uint16_t queue) {
	int r;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&gmutex);
#endif
	if (globaladdr<0) globaladdr = sockaddrnewempty();
	sockaddrchange(globaladdr,hostname,service,"tcp");
	r = bind(sock, (struct sockaddr *)(addrtab+globaladdr),sizeof(struct sockaddr_in));
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&gmutex);
#endif
	if (r<0) {
		return -1;
	}
	if (listen(sock,queue)<0) {
		return -1;
	}
	return 0;
}

int tcpaddrlisten(int sock,uint32_t addr,uint16_t queue) {
	if (addr>addrlen) return -1;
	if (bind(sock, (struct sockaddr *)(addrtab+addr),sizeof(struct sockaddr_in))<0) {
		return -1;
	}
	if (listen(sock,queue)<0) {
		return -1;
	}
	return 0;
}

int tcpaccept(int lsock) {
	int sock;
	sock=accept(lsock,(struct sockaddr *)NULL,0);
	if (sock<0) return -1;
	return sock;
}

int tcpgetpeer(int sock,uint32_t *ip,uint16_t *port) {
	struct sockaddr_in	iaddr;
	socklen_t leng;
	leng=sizeof(iaddr);
	if (getpeername(sock,(struct sockaddr *)&iaddr,&leng)<0) return -1;
	if (ip!=(void *)0) {
		*ip = ntohl(iaddr.sin_addr.s_addr);
	}
	if (port!=(void *)0) {
		*port = ntohs(iaddr.sin_port);
	}
	return 0;
}

/*
int tcpdisconnect(int sock) {
	return close(sock);
}
*/

int tcpclose(int sock) {
#ifndef WIN32
	return close(sock);
#else
	return closesocket(sock);
#endif
}

int32_t tcpread(int sock,void *buff,uint32_t leng) {
	uint32_t rcvd=0;
	int i;
	while (rcvd<leng) {
#ifndef WIN32
		i = read(sock,((uint8_t*)buff)+rcvd,leng-rcvd);
#else
		i = recv(sock,((char *)buff)+rcvd,leng-rcvd,0);
#endif
		if (i<=0) return i;
		rcvd+=i;
	}
	return rcvd;
}

int32_t tcpwrite(int sock,const void *buff,uint32_t leng) {
	uint32_t sent=0;
	int i;
	while (sent<leng) {
#ifndef WIN32
		i = write(sock,((const uint8_t*)buff)+sent,leng-sent);
#else
		i = send(sock,((const char*)buff)+sent,leng-sent,0);
#endif
		if (i<=0) return i;
		sent+=i;
	}
	return sent;
}

#ifndef WIN32
int32_t tcptoread(int sock,void *buff,uint32_t leng,uint32_t msecto) {
	uint32_t rcvd=0;
	int i;
	struct pollfd pfd;
	pfd.fd = sock;
	pfd.events = POLLIN;
	while (rcvd<leng) {
		pfd.revents = 0;
		if (poll(&pfd,1,msecto)<0) {
			return -1;
		}
		if (pfd.revents & POLLIN) {
			i = read(sock,((uint8_t*)buff)+rcvd,leng-rcvd);
			if (i<=0) return i;
			rcvd+=i;
		} else {
			errno = ETIMEDOUT;
			return -1;
		}
	}
	return rcvd;
}

int32_t tcptowrite(int sock,const void *buff,uint32_t leng,uint32_t msecto) {
	uint32_t sent=0;
	int i;
	struct pollfd pfd;
	pfd.fd = sock;
	pfd.events = POLLOUT;
	while (sent<leng) {
		pfd.revents = 0;
		if (poll(&pfd,1,msecto)<0) {
			return -1;
		}
		if (pfd.revents & POLLOUT) {
			i = write(sock,((uint8_t*)buff)+sent,leng-sent);
			if (i<=0) return i;
			sent+=i;
		} else {
			errno = ETIMEDOUT;
			return -1;
		}
	}
	return sent;
}
#endif

/* ----------------- UDP ----------------- */
/*
int udpaddrsocket(int addr) {
	int sock;
	if ((sock = socket(AF_INET,SOCK_DGRAM,0))<0)
		return -1;
	if (bind(sock, (struct sockaddr *)addrtab+addr,sizeof(struct sockaddr_in)))
		return -1;
	return sock;
}
*/

int udpsocket(void) {
	return socket(AF_INET,SOCK_DGRAM,0);
}

int udpnumlisten(int sock,uint32_t ip,uint16_t port) {
	int r;
#ifdef _THREAD_SAFE
	pthread_mutex_lock(&gmutex);
#endif
	if (globaladdr<0) globaladdr = sockaddrnewempty();
	sockaddrnumchange(globaladdr,ip,port);
	r = bind(sock, (struct sockaddr *)(addrtab+globaladdr),sizeof(struct sockaddr_in));
#ifdef _THREAD_SAFE
	pthread_mutex_unlock(&gmutex);
#endif
	return r;
}

int udplisten(int sock,const char *hostname,const char *service) {
	int r;
#ifdef _THREAD_SAFE
	    pthread_mutex_lock(&gmutex);
#endif
	if (globaladdr<0) globaladdr = sockaddrnewempty();
	sockaddrchange(globaladdr,hostname,service,"udp");
	r = bind(sock, (struct sockaddr *)(addrtab+globaladdr),sizeof(struct sockaddr_in));
#ifdef _THREAD_SAFE
	    pthread_mutex_unlock(&gmutex);
#endif
	return r;
}

int udpaddrlisten(int sock,uint32_t addr) {
	if (addr>addrlen) return -1;
	return bind(sock, (struct sockaddr *)(addrtab+addr),sizeof(struct sockaddr_in));
}

int udpwrite(int sock,uint32_t addr,const void *buff,uint16_t leng) {
	if (addr>addrlen || leng>512) return -1;
#ifndef WIN32
	return sendto(sock,buff,leng,0,(struct sockaddr *)(addrtab+addr),sizeof(struct sockaddr_in));
#else
	return sendto(sock,(const char*)buff,leng,0,(struct sockaddr *)(addrtab+addr),sizeof(struct sockaddr_in));
#endif
}

int udpread(int sock,uint32_t addr,void *buff,uint16_t leng) {
	if (addr>addrlen) return -1;
	if (addr==0xffffffff) {	// ignore peer name
#ifndef WIN32
		return recvfrom(sock,buff,leng,0,(struct sockaddr *)NULL,0);
#else
		return recvfrom(sock,(char*)buff,leng,0,(struct sockaddr *)NULL,0);
#endif
	} else {
		socklen_t templeng;
		struct sockaddr tempaddr;
#ifndef WIN32
		return recvfrom(sock,buff,leng,0,&tempaddr,&templeng);
#else
		return recvfrom(sock,(char*)buff,leng,0,&tempaddr,&templeng);
#endif
		if (templeng==sizeof(struct sockaddr_in)) {
			addrtab[addr] = *((struct sockaddr_in*)&tempaddr);
		}
	}
}

int udpclose(int sock) {
#ifndef WIN32
	return close(sock);
#else
	return closesocket(sock);
#endif
}

