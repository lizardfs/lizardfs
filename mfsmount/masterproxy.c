#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>

#include "sockets.h"
#include "mastercomm.h"
#include "datapack.h"
#include "MFSCommunication.h"

#define QUERYSIZE 10000
#define ANSSIZE 10000

static int lsock = -1;
static pthread_t proxythread;
static uint8_t terminate;

static uint32_t proxyhost;
static uint16_t proxyport;


/*
void* masterproxy_loop(void* args) {
	struct pollfd pdesc[MAXFILES];
	uint32_t ndesc;

	(void)args;

	while (terminate==0) {
		ndesc = 0;
		masterproxy_desc(pdesc,&ndesc);
		i = poll(pdesc,ndesc,50);
//		gettimeofday(&tv,NULL);
//		usecnow = tv.tv_sec;
//		usecnow *= 1000000;
//		usecnow += tv.tv_usec;
//		now = tv.tv_sec;
		if (i<0) {
			if (errno==EAGAIN) {
				syslog(LOG_WARNING,"poll returned EAGAIN");
				usleep(100000);
			}
			if (errno!=EINTR) {
				syslog(LOG_WARNING,"poll error: %s",strerr(errno));
				usleep(100000);
			}
			continue;
		} else {
			masterproxy_serve(pdesc);
		}
	}
	return NULL;
}
*/

void masterproxy_getlocation(uint8_t *masterinfo) {
	const uint8_t *rptr = masterinfo+10;
	if (lsock>=0 && get32bit(&rptr)>=0x00010618) {	// use proxy only when master version is greater than or equal to 1.6.24
		put32bit(&masterinfo,proxyhost);
		put16bit(&masterinfo,proxyport);
	}
}

static void* masterproxy_server(void *args) {
	uint8_t header[8];
	uint8_t querybuffer[QUERYSIZE];
	uint8_t ansbuffer[ANSSIZE];
	uint8_t *wptr;
	const uint8_t *rptr;
	int sock = *((int*)args);
	uint32_t psize,cmd,msgid,asize,acmd;

	free(args);

	for (;;) {
		if (tcptoread(sock,header,8,1000)!=8) {
			tcpclose(sock);
			return NULL;
		}

		rptr = header;
		cmd = get32bit(&rptr);
		psize = get32bit(&rptr);
		if (cmd==CLTOMA_FUSE_REGISTER) {	// special case: register
			// if (psize>QUERYSIZE) {
			if (psize!=73) {
				tcpclose(sock);
				return NULL;
			}

			if (tcptoread(sock,querybuffer,psize,1000)!=(int32_t)(psize)) {
				tcpclose(sock);
				return NULL;
			}

			if (memcmp(querybuffer,FUSE_REGISTER_BLOB_ACL,64)!=0) {
				tcpclose(sock);
				return NULL;
			}

			if (querybuffer[64]!=REGISTER_TOOLS) {
				tcpclose(sock);
				return NULL;
			}

			wptr = ansbuffer;
			put32bit(&wptr,MATOCL_FUSE_REGISTER);
			put32bit(&wptr,1);
			put8bit(&wptr,STATUS_OK);

			if (tcptowrite(sock,ansbuffer,9,1000)!=9) {
				tcpclose(sock);
				return NULL;
			}
		} else {
			if (psize<4 || psize>QUERYSIZE) {
				tcpclose(sock);
				return NULL;
			}

			if (tcptoread(sock,querybuffer,psize,1000)!=(int32_t)(psize)) {
				tcpclose(sock);
				return NULL;
			}

			rptr = querybuffer;
			msgid = get32bit(&rptr);

			asize = ANSSIZE-12;
			if (fs_custom(cmd,querybuffer+4,psize-4,&acmd,ansbuffer+12,&asize)!=STATUS_OK) {
				tcpclose(sock);
				return NULL;
			}

			wptr = ansbuffer;
			put32bit(&wptr,acmd);
			put32bit(&wptr,asize+4);
			put32bit(&wptr,msgid);

			if (tcptowrite(sock,ansbuffer,asize+12,1000)!=(int32_t)(asize+12)) {
				tcpclose(sock);
				return NULL;
			}
		}
	}
}

static void* masterproxy_acceptor(void *args) {
	pthread_t clientthread;
	pthread_attr_t thattr;
	int sock;
	(void)args;

	pthread_attr_init(&thattr);
	pthread_attr_setstacksize(&thattr,0x100000);
	pthread_attr_setdetachstate(&thattr,PTHREAD_CREATE_DETACHED);

	while (terminate==0) {
		sock = tcptoaccept(lsock,1000);
		if (sock>=0) {
			int *s = malloc(sizeof(int));
			// memory is freed inside pthread routine !!!
			*s = sock;
			tcpnodelay(sock);
			if (pthread_create(&clientthread,&thattr,masterproxy_server,s)<0) {
				free(s);
				tcpclose(sock);
			}
		}
	}

	pthread_attr_destroy(&thattr);
	return NULL;
}

void masterproxy_term(void) {
	terminate=1;
	pthread_join(proxythread,NULL);
}

int masterproxy_init(void) {
	pthread_attr_t thattr;

	lsock = tcpsocket();
	if (lsock<0) {
		//mfs_errlog(LOG_ERR,"main master server module: can't create socket");
		return -1;
	}
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	// tcpreuseaddr(lsock);
	if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
		// mfs_errlog_silent(LOG_NOTICE,"master proxy: can't set accept filter");
	}
	if (tcpstrlisten(lsock,"127.0.0.1",0,100)<0) {
		// mfs_errlog(LOG_ERR,"main master server module: can't listen on socket");
		tcpclose(lsock);
		lsock = -1;
		return -1;
	}
	if (tcpgetmyaddr(lsock,&proxyhost,&proxyport)<0) {
		tcpclose(lsock);
		lsock = -1;
		return -1;
	}

	terminate = 0;
	pthread_attr_init(&thattr);
	pthread_attr_setstacksize(&thattr,0x100000);
	//pthread_create(&proxythread,&thattr,masterproxy_loop,NULL);
	pthread_create(&proxythread,&thattr,masterproxy_acceptor,NULL);
	pthread_attr_destroy(&thattr);

	return 1;
}
