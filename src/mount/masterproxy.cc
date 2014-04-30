#include "config.h"
#include "mount/masterproxy.h"

#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <vector>

#include "common/datapack.h"
#include "common/MFSCommunication.h"
#include "common/packet.h"
#include "common/sockets.h"
#include "mount/mastercomm.h"

static int lsock = -1;
static pthread_t proxythread;
static uint8_t terminate;

static uint32_t proxyhost;
static uint16_t proxyport;

void masterproxy_getlocation(uint8_t *masterinfo) {
	const uint8_t *rptr = masterinfo+10;
	if (lsock>=0 && get32bit(&rptr)>=0x00010618) {  // use proxy only when master version is greater than or equal to 1.6.24
		put32bit(&masterinfo,proxyhost);
		put16bit(&masterinfo,proxyport);
	}
}

static void* masterproxy_server(void *args) {
	std::vector<uint8_t> buffer;
	int sock = *((int*)args);
	free(args);

	for (;;) {
		PacketHeader header;
		const int32_t headerSize = serializedSize(header);

		buffer.resize(headerSize);
		if (tcptoread(sock, buffer.data(), headerSize, 1000) != headerSize) {
			tcpclose(sock);
			return NULL;
		}

		deserializePacketHeader(buffer, header);

		const int32_t payloadSize = header.length;
		buffer.resize(headerSize + payloadSize);

		if (tcptoread(sock, buffer.data() + headerSize, payloadSize, 1000) != payloadSize) {
			tcpclose(sock);
			return NULL;
		}

		if (header.type == CLTOMA_FUSE_REGISTER) {      // special case: register
			const uint8_t *payload = buffer.data() + headerSize;
			if (payloadSize != 73) {
				tcpclose(sock);
				return NULL;
			}
			if (memcmp(payload, FUSE_REGISTER_BLOB_ACL, 64) != 0) {
				tcpclose(sock);
				return NULL;
			}
			if (payload[64] != REGISTER_TOOLS) {
				tcpclose(sock);
				return NULL;
			}

			buffer.clear();
			serializeMooseFsPacket(buffer, MATOCL_FUSE_REGISTER, uint8_t(STATUS_OK));

		} else {
			if (fs_custom(buffer) != STATUS_OK) {
				tcpclose(sock);
				return NULL;
			}
		}

		const int32_t size = buffer.size();
		if (tcptowrite(sock, buffer.data(), size, 1000) != size) {
			tcpclose(sock);
			return NULL;
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
			int *s = (int*) malloc(sizeof(int));
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
	pthread_create(&proxythread,&thattr,masterproxy_acceptor,NULL);
	pthread_attr_destroy(&thattr);

	return 1;
}
