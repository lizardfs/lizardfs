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

#ifndef _SOCKETS_H_
#define _SOCKETS_H_

#include <inttypes.h>

/* ---------------SOCK ADDR--------------- */

int32_t sockaddrnewempty();
int sockaddrchange(uint32_t i,const char *hostname,const char *service,const char *proto);
int sockaddrnumchange(uint32_t i,uint32_t ip,uint16_t port);
int32_t sockaddrnew(const char *hostname,const char *service,const char *proto);
int sockaddrget(uint32_t i,uint32_t *ip,uint16_t *port);
int sockaddrconvert(const char *hostname,const char *service,const char *proto,uint32_t *ip,uint16_t *port);

/* ---------------- SOCKET --------------- */

int socknonblock(int sock);

/* ----------------- TCP ----------------- */

int tcpsocket(void);
#define tcpaddrconvert(h,s,i,p) sockaddrconvert(h,s,"tcp",i,p)
#define tcpnonblock	socknonblock
int tcpreuseaddr(int sock);
int tcpnodelay(int sock);
int tcpaccfhttp(int sock);
int tcpaccfdata(int sock);
int tcpnumconnect(int sock,uint32_t ip,uint16_t port);
int tcpconnect(int sock,const char *hostname,const char *service);
int tcpaddrconnect(int sock,uint32_t addr);
int tcpgetstatus(int sock);
int tcpnumlisten(int sock,uint32_t ip,uint16_t port,uint16_t queue);
int tcplisten(int sock,const char *hostname,const char *service,uint16_t queue);
int tcpaddrlisten(int sock,uint32_t addr,uint16_t queue);
int tcpaccept(int lsock);
int tcpgetpeer(int sock,uint32_t *ip,uint16_t *port);
//int tcpdisconnect(int sock);
int tcpclose(int sock);
int32_t tcpread(int sock,void *buff,uint32_t leng);
int32_t tcpwrite(int sock,const void *buff,uint32_t leng);
int32_t tcptoread(int sock,void *buff,uint32_t leng,uint32_t msecto);
int32_t tcptowrite(int sock,const void *buff,uint32_t leng,uint32_t msecto);

/* ----------------- UDP ----------------- */

#define ADDR_IGNORE 0xffffffff

int udpsocket(void);
#define udpaddrconvert(h,s,i,p) sockaddrconvert(h,s,"udp",i,p)
#define udpnonblock socknonblock
int udpnumlisten(int sock,uint32_t ip,uint16_t port);
int udplisten(int sock,const char *hostname,const char *service);
int udpaddrlisten(int sock,uint32_t addr);
int udpwrite(int sock,uint32_t addr,const void *buff,uint16_t leng);
int udpread(int sock,uint32_t addr,void *buff,uint16_t leng);
int udpclose(int sock);

/* ----------------- EXAMPLES ------------------
 * tcpserver:
 * 
 * 	int sock;
 * 	sock = tcpsocket();
 * 	tcpreuseaddr(sock);
 * 	tcplisten(sock,"*","http",5);
 * 	do {
 * 		(select sock)
 * 		if CANREAD(sock) {
 * 			int csock = tcpaccept(sock);
 * 			int ip;
 * 			int port;
 * 			tcpgetpeer(csock,&ip,&port);
 * 			( serve connection from ip,port )
 *		}
 *	} while (!exit);
 *	tcpdisconnect(sock);
 *
 * ---------------------------------------------
 * tcpclient: (blocking mode)
 *
 *	int addr;
 *	sock = tcpsocket()
 *	tcpconnect(sock,"www.google.com","http");
 *	tcpwrite(sock,"GET / HTTP/1.0\r\nHost: www.google.com\r\n\r\n",40);
 *	tcpread(sock,buff,maxleng);
 *	tcpdisconnect(sock);
 *
 * ---------------------------------------------
 * tcpclient: (non blocking mode)
 * 
 * 	int sock;
 * 	sock = tcpsocket();
 * 	tcpnonblock(sock);
 *	switch (tcpconnect(sock,"www.google.com","http")) {
 *	case 1: // connectiong in progress
 *  		(select sock)
 *		if CANWRITE(sock) {
 *			if tcpgetstatus(sock) {
 *				error
 *			} else {
 *				connected - do something
 *			}
 *		}
 *		break;
 *	case 0: // connected imediately
 *		connected - do something
 *		break;
 *	case -1:
 *		error
 *	}
 *	tcpdisconnect(sock);
 *
 * --------------------------------------------
 * udpserver:
 *
 *	int sock;
 *	int addr;
 *	addr = newsockaddr("*","dns","udp");	// "*" means any ip
 *	sock = udpsocket(addr);
 *	    ....
 *	if CANREAD(sock) {
 *		int ip,port;
 *		leng = udpread(sock,addr,buff,maxleng);		//changes addr
 *		getsockaddr(addr,&ip,&port);
 *	}
 *	udpwrite(sock,addr,buff,leng);		// answer to the same address (addr)
 *	....
 *	udpclose(sock);
 *
 * --------------------------------------------
 * udpclient:
 *
 * 	int sock;
 * 	int addr;
 * 	addr = newsockaddr("*","*","");		// "*","*" means any ip, any port
 * 	sock = udpsocket(addr);
 * 	changesockaddr(addr,"192.5.6.30","dns","udp");
 * 	udpwrite(sock,addr,buff,leng);
 * 	(select sock)
 * 	if CANREAD(sock) {
 * 		leng = udpread(sock,-1,buff,maxleng);		// -1 means: forget sender address
 * 	}
 * 	udpclose(sock);
 */

#endif
