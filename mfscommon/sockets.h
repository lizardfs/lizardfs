/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA.

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

/* ----------------- TCP ----------------- */

int tcpsocket(void);
int tcpresolve(const char *hostname,const char *service,uint32_t *ip,uint16_t *port,int passiveflag);
int tcpnonblock(int sock);
int tcpsetacceptfilter(int sock);
int tcpreuseaddr(int sock);
int tcpnodelay(int sock);
int tcpaccfhttp(int sock);
int tcpaccfdata(int sock);
int tcpnumbind(int sock,uint32_t ip,uint16_t port);
int tcpstrbind(int sock,const char *hostname,const char *service);
int tcpnumconnect(int sock,uint32_t ip,uint16_t port);
int tcpnumtoconnect(int sock,uint32_t ip,uint16_t port,uint32_t msecto);
int tcpstrconnect(int sock,const char *hostname,const char *service);
int tcpstrtoconnect(int sock,const char *hostname,const char *service,uint32_t msecto);
int tcpgetstatus(int sock);
int tcpnumlisten(int sock,uint32_t ip,uint16_t port,uint16_t queue);
int tcpstrlisten(int sock,const char *hostname,const char *service,uint16_t queue);
int tcpaccept(int lsock);
int tcpgetpeer(int sock,uint32_t *ip,uint16_t *port);
int tcpgetmyaddr(int sock,uint32_t *ip,uint16_t *port);
int tcpclose(int sock);
//int32_t tcpread(int sock,void *buff,uint32_t leng);
//int32_t tcpwrite(int sock,const void *buff,uint32_t leng);
int32_t tcptoread(int sock,void *buff,uint32_t leng,uint32_t msecto);
int32_t tcptowrite(int sock,const void *buff,uint32_t leng,uint32_t msecto);
int tcptoaccept(int sock,uint32_t msecto);

/* ----------------- UDP ----------------- */

int udpsocket(void);
int udpresolve(const char *hostname,const char *service,uint32_t *ip,uint16_t *port,int passiveflag);
int udpnonblock(int sock);
int udpnumlisten(int sock,uint32_t ip,uint16_t port);
int udpstrlisten(int sock,const char *hostname,const char *service);
int udpwrite(int sock,uint32_t ip,uint16_t port,const void *buff,uint16_t leng);
int udpread(int sock,uint32_t *ip,uint16_t *port,void *buff,uint16_t leng);
int udpclose(int sock);

#endif
