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

#include "common/platform.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>
#include <atomic>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <tuple>

#include "chunkserver/bgjobs.h"
#include "chunkserver/g_limiters.h"
#include "chunkserver/hdd_readahead.h"
#include "chunkserver/network_main_thread.h"
#include "chunkserver/network_stats.h"
#include "chunkserver/network_worker_thread.h"
#include "chunkserver/chunk_replicator.h"
#include "common/cfg.h"
#include "common/charts.h"
#include "common/event_loop.h"
#include "protocol/cltocs.h"
#include "protocol/cstocl.h"
#include "protocol/cstocs.h"
#include "common/cwrap.h"
#include "common/datapack.h"
#include "common/exceptions.h"
#include "common/main.h"
#include "common/massert.h"
#include "protocol/MFSCommunication.h"
#include "protocol/packet.h"
#include "common/slogger.h"
#include "common/sockets.h"
#include "devtools/TracePrinter.h"

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

void chunkReplicatorReload() {
	unsigned rep_total = cfg_get_minmaxvalue<unsigned>("REPLICATION_TOTAL_TIMEOUT_MS",
	                                                   ChunkReplicator::kDefaultTotalTimeout_ms,
	                                                   1000, 60 * 60 * 1000);
	unsigned rep_wave = cfg_get_minmaxvalue<unsigned>("REPLICATION_WAVE_TIMEOUT_MS",
	                                                  ChunkReplicator::kDefaultWaveTimeout_ms,
	                                                  50, 30 * 1000);
	unsigned rep_connection = cfg_get_minmaxvalue<unsigned>("REPLICATION_CONNECTION_TIMEOUT_MS",
	                                                        ChunkReplicator::kDefaultConnectionTimeout_ms,
	                                                        200, 30 * 1000);

	gReplicator.setTotalTimeout(rep_total);
	gReplicator.setWaveTimeout(rep_wave);
	gReplicator.setConnectionTimeout(rep_connection);
}

void replicationBandwidthLimitReload() {
	if (cfg_isdefined("REPLICATION_BANDWIDTH_LIMIT_KBPS")) {
		replicationBandwidthLimiter().setLimit(cfg_getuint32("REPLICATION_BANDWIDTH_LIMIT_KBPS", 0));
	} else {
		replicationBandwidthLimiter().unsetLimit();
	}
}

void mainNetworkThreadReload(void) {
	TRACETHIS();

	cfg_warning_on_value_change(
			"NR_OF_NETWORK_WORKERS", gNrOfNetworkWorkers);
	cfg_warning_on_value_change(
			"NR_OF_HDD_WORKERS_PER_NETWORK_WORKER", gNrOfHddWorkersPerNetworkWorker);
	cfg_warning_on_value_change(
			"BGJOBSCNT_PER_NETWORK_WORKER", gBgjobsCountPerNetworkWorker);

	try {
		replicationBandwidthLimitReload();
	} catch (std::exception& ex) {
		lzfs_pretty_errlog(LOG_ERR,
				"main server module: can't reload REPLICATION_BANDWIDTH_LIMIT_KBPS: %s",
				ex.what());
	}
	chunkReplicatorReload();

	gHDDReadAhead.setReadAhead_kB(
			cfg_get_maxvalue<uint32_t>("READ_AHEAD_KB", 0, MFSCHUNKSIZE / 1024));
	gHDDReadAhead.setMaxReadBehind_kB(
			cfg_get_maxvalue<uint32_t>("MAX_READ_BEHIND_KB", 0, MFSCHUNKSIZE / 1024));

	char *oldListenHost, *oldListenPort;
	int newlsock;
	oldListenHost = ListenHost;
	oldListenPort = ListenPort;
	ListenHost = cfg_getstr("CSSERV_LISTEN_HOST", "*");
	ListenPort = cfg_getstr("CSSERV_LISTEN_PORT", "9422");
	if (strcmp(oldListenHost, ListenHost) == 0 && strcmp(oldListenPort, ListenPort) == 0) {
		free(oldListenHost);
		free(oldListenPort);
		lzfs_pretty_syslog(LOG_NOTICE,
				"main server module: socket address hasn't changed (%s:%s)",
				ListenHost, ListenPort);
		return;
	}

	newlsock = tcpsocket();
	if (newlsock < 0) {
		lzfs_pretty_errlog(LOG_WARNING,
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
		lzfs_silent_errlog(LOG_NOTICE, "main server module: can't set accept filter");
	}
	if (tcpstrlisten(newlsock, ListenHost, ListenPort, 100) < 0) {
		lzfs_pretty_errlog(LOG_ERR,
				"main server module: socket address has changed, but can't listen on socket (%s:%s)",
				ListenHost, ListenPort);
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		tcpclose(newlsock);
		return;
	}
	lzfs_pretty_syslog(LOG_NOTICE,
			"main server module: socket address has changed, now listen on %s:%s",
			ListenHost, ListenPort);
	free(oldListenHost);
	free(oldListenPort);
	tcpclose(lsock);
	lsock = newlsock;
}

void mainNetworkThreadDesc(std::vector<pollfd> &pdesc) {
	TRACETHIS();
	pdesc.push_back({lsock, POLLIN, 0});
	lsockpdescpos = pdesc.size() - 1;
}

void mainNetworkThreadTerm(void) {
	TRACETHIS();
	lzfs_pretty_syslog(LOG_NOTICE, "closing %s:%s", ListenHost, ListenPort);
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

void mainNetworkThreadServe(const std::vector<pollfd> &pdesc) {
	TRACETHIS();
	int newSocketFD;

	if (lsockpdescpos >= 0 && (pdesc[lsockpdescpos].revents & POLLIN)) {
		newSocketFD = tcpaccept(lsock);
		if (newSocketFD < 0) {
			lzfs_silent_errlog(LOG_NOTICE, "accept error");
		} else {
			if (nextNetworkThread == networkThreadObjects.end()) {
				nextNetworkThread = networkThreadObjects.begin();
			}
			if (job_pool_jobs_count(nextNetworkThread->bgJobPool())
					>= (gBgjobsCountPerNetworkWorker * 9) / 10) {
				lzfs_pretty_syslog(LOG_WARNING, "jobs queue is full !!!");
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
			"NR_OF_HDD_WORKERS_PER_NETWORK_WORKER", 2, 1);
	gBgjobsCountPerNetworkWorker = cfg_get_minvalue<uint32_t>(
			"BGJOBSCNT_PER_NETWORK_WORKER", 1000, 10);

	gHDDReadAhead.setReadAhead_kB(
			cfg_get_maxvalue<uint32_t>("READ_AHEAD_KB", 0, MFSCHUNKSIZE / 1024));
	gHDDReadAhead.setMaxReadBehind_kB(
			cfg_get_maxvalue<uint32_t>("MAX_READ_BEHIND_KB", 0, MFSCHUNKSIZE / 1024));

	lsock = tcpsocket();
	if (lsock < 0) {
		throw InitializeException("main server module: can't create socket :" +
				errorString(errno));
	}
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	tcpreuseaddr(lsock);
	if (tcpsetacceptfilter(lsock) < 0 && errno != ENOTSUP) {
		lzfs_silent_errlog(LOG_NOTICE, "main server module: can't set accept filter");
	}
	tcpresolve(ListenHost, ListenPort, &mylistenip, &mylistenport, 1);
	if (tcpnumlisten(lsock, mylistenip, mylistenport, 100) < 0) {
		throw InitializeException("main server module: can't listen on socket" +
				errorString(errno));
	}
	lzfs_pretty_syslog(LOG_NOTICE, "main server module: listen on %s:%s", ListenHost, ListenPort);

	eventloop_reloadregister(mainNetworkThreadReload);
	eventloop_destructregister(mainNetworkThreadTerm);
	eventloop_pollregister(mainNetworkThreadDesc, mainNetworkThreadServe);

	try {
		replicationBandwidthLimitReload();
	} catch (Exception& e) {
		throw InitializeException("can't initialize replication bandwidth limiter: " + e.message());
	}
	chunkReplicatorReload();

	return 0;
}

int mainNetworkThreadInitThreads(void) {
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
