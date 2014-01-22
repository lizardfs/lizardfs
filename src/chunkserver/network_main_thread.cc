#include "config.h"

#include "chunkserver/network_main_thread.h"

#include <fcntl.h>
#include <time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <tuple>

#include "devtools/TracePrinter.h"
#include "chunkserver/bgjobs.h"
#include "chunkserver/network_stats.h"
#include "chunkserver/network_worker_thread.h"
#include "common/cfg.h"
#include "common/charts.h"
#include "common/cltocs_communication.h"
#include "common/cstocl_communication.h"
#include "common/cstocs_communication.h"
#include "common/datapack.h"
#include "common/massert.h"
#include "common/main.h"
#include "common/MFSCommunication.h"
#include "common/packet.h"
#include "common/slogger.h"
#include "common/sockets.h"

static int lsock;
static int32_t lsockpdescpos;

std::list<std::thread> networkThreads;
std::list<NetworkWorkerThread> networkThreadObjects;
std::list<NetworkWorkerThread>::iterator nextNetworkThread;

static uint32_t mylistenip;
static uint16_t mylistenport;

// from config
static char *ListenHost;
static char *ListenPort;
static uint32_t gNrOfNetworkWorkers;
static uint32_t gNrOfHddWorkersPerNetworkWorker;
static uint32_t gBgjobsCountPerNetworkWorker;

void mainNetworkThreadReload(void) {
	TRACETHIS();

	cfg_warning_on_value_change(
			"NR_OF_NETWORK_WORKERS", gNrOfNetworkWorkers);
	cfg_warning_on_value_change(
			"NR_OF_HDD_WORKERS_PER_NETWORK_WORKER", gNrOfHddWorkersPerNetworkWorker);
	cfg_warning_on_value_change(
			"BGJOBSCNT_PER_NETWORK_WORKER", gBgjobsCountPerNetworkWorker);

	char *oldListenHost, *oldListenPort;
	int newlsock;
	oldListenHost = ListenHost;
	oldListenPort = ListenPort;
	ListenHost = cfg_getstr("CSSERV_LISTEN_HOST", "*");
	ListenPort = cfg_getstr("CSSERV_LISTEN_PORT", "9422");
	if (strcmp(oldListenHost, ListenHost) == 0 && strcmp(oldListenPort, ListenPort) == 0) {
		free(oldListenHost);
		free(oldListenPort);
		mfs_arg_syslog(LOG_NOTICE,
				"main server module: socket address hasn't changed (%s:%s)",
				ListenHost, ListenPort);
		return;
	}

	newlsock = tcpsocket();
	if (newlsock < 0) {
		mfs_errlog(LOG_WARNING,
				"main server module: socket address has changed, but can't create new socket");
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		return;
	}
	tcpnonblock(newlsock);
	tcpnodelay(newlsock);
	tcpreuseaddr(newlsock);
	if (tcpsetacceptfilter(newlsock) < 0 && errno != ENOTSUP) {
		mfs_errlog_silent(LOG_NOTICE, "main server module: can't set accept filter");
	}
	if (tcpstrlisten(newlsock, ListenHost, ListenPort, 100) < 0) {
		mfs_arg_errlog(LOG_ERR,
				"main server module: socket address has changed, but can't listen on socket (%s:%s)",
				ListenHost, ListenPort);
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		tcpclose(newlsock);
		return;
	}
	mfs_arg_syslog(LOG_NOTICE,
			"main server module: socket address has changed, now listen on %s:%s",
			ListenHost, ListenPort);
	free(oldListenHost);
	free(oldListenPort);
	tcpclose(lsock);
	lsock = newlsock;
}

void mainNetworkThreadDesc(struct pollfd *pdesc, uint32_t *ndesc) {
	TRACETHIS();
	uint32_t pos = *ndesc;
	pdesc[pos].fd = lsock;
	pdesc[pos].events = POLLIN;
	lsockpdescpos = pos;
	pos++;
	*ndesc = pos;
}

void mainNetworkThreadTerm(void) {
	TRACETHIS();
	syslog(LOG_NOTICE, "closing %s:%s", ListenHost, ListenPort);
	tcpclose(lsock);

	free(ListenHost);
	free(ListenPort);

	for (auto& threadObject : networkThreadObjects) {
		threadObject.askForTermination();
	}
	for (auto& thread : networkThreads) {
		thread.join();
	}
}

void mainNetworkThreadServe(struct pollfd *pdesc) {
	TRACETHIS();
	int newSocketFD;

	if (lsockpdescpos >= 0 && (pdesc[lsockpdescpos].revents & POLLIN)) {
		newSocketFD = tcpaccept(lsock);
		if (newSocketFD < 0) {
			mfs_errlog_silent(LOG_NOTICE, "accept error");
		} else {
			if (nextNetworkThread == networkThreadObjects.end()) {
				nextNetworkThread = networkThreadObjects.begin();
			}
			if (job_pool_jobs_count(nextNetworkThread->bgJobPool())
					>= (gNrOfHddWorkersPerNetworkWorker * 9) / 10) {
				syslog(LOG_WARNING, "jobs queue is full !!!");
				tcpclose(newSocketFD);
			} else {
				nextNetworkThread->addConnection(newSocketFD);
			}
			++nextNetworkThread;
		}
	}
}

int mainNetworkThreadInit(void) {
	TRACETHIS();
	ListenHost = cfg_getstr("CSSERV_LISTEN_HOST", "*");
	ListenPort = cfg_getstr("CSSERV_LISTEN_PORT", "9422");

	gNrOfNetworkWorkers = cfg_get_minvalue<uint32_t>("NR_OF_NETWORK_WORKERS", 1, 1);
	gNrOfHddWorkersPerNetworkWorker = cfg_get_minvalue<uint32_t>(
			"NR_OF_HDD_WORKERS_PER_NETWORK_WORKER", 20, 1);
	gBgjobsCountPerNetworkWorker = cfg_get_minvalue<uint32_t>(
			"BGJOBSCNT_PER_NETWORK_WORKER", 1000, 10);

	lsock = tcpsocket();
	if (lsock < 0) {
		mfs_errlog(LOG_ERR, "main server module: can't create socket");
		return -1;
	}
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	tcpreuseaddr(lsock);
	if (tcpsetacceptfilter(lsock) < 0 && errno != ENOTSUP) {
		mfs_errlog_silent(LOG_NOTICE, "main server module: can't set accept filter");
	}
	tcpresolve(ListenHost, ListenPort, &mylistenip, &mylistenport, 1);
	if (tcpnumlisten(lsock, mylistenip, mylistenport, 100) < 0) {
		mfs_errlog(LOG_ERR, "main server module: can't listen on socket");
		return -1;
	}
	mfs_arg_syslog(LOG_NOTICE, "main server module: listen on %s:%s", ListenHost, ListenPort);

	main_reloadregister(mainNetworkThreadReload);
	main_destructregister(mainNetworkThreadTerm);
	main_pollregister(mainNetworkThreadDesc, mainNetworkThreadServe);

	for (unsigned i = 0; i < gNrOfNetworkWorkers; ++i) {
		networkThreadObjects.emplace_back(gNrOfHddWorkersPerNetworkWorker,
				gBgjobsCountPerNetworkWorker);
	}
	for (auto obj = networkThreadObjects.begin(); obj != networkThreadObjects.end(); ++obj) {
		networkThreads.push_back(std::thread(std::ref(*obj)));
	}
	sassert(!networkThreads.empty());
	nextNetworkThread = networkThreadObjects.end();

	return 0;
}

uint32_t mainNetworkThreadGetListenIp() {
	TRACETHIS();
	return mylistenip;
}

uint16_t mainNetworkThreadGetListenPort() {
	TRACETHIS();
	return mylistenport;
}
