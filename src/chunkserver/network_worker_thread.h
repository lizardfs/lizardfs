#pragma once

#include <inttypes.h>

#include <atomic>
#include <list>
#include <mutex>
#include <set>
#include <vector>

#include "common/chunk_type.h"
#include "common/packet.h"
#include "common/network_address.h"
#include "chunkserver/network_stats.h"
#include "chunkserver/output_buffers.h"
#include "devtools/request_log.h"

//entry.mode
enum ChunkserverEntryMode {
	HEADER, DATA
};

//entry.state
enum ChunkserverEntryState {
	IDLE,        // idle connection, new or used previously
	READ,        // after CLTOCS_READ, but didn't send all of the CSTOCL_READ_(DATA|STAUS)
	GET_BLOCK,   // after CSTOCS_GET_CHUNK_BLOCKS, but didn't send response
	WRITELAST,   // connection ready for writing data; data is not forwarded to other chunkservers
	CONNECTING,  // we are now connecting to other chunkserver to form a writing chain
	WRITEINIT,   // we are sending packet forming a chain to the next chunkserver
	WRITEFWD,    // connection ready for writing data; data will be forwarded to other chunkservers
	WRITEFINISH, // write error occurred, will be closed after sending error status
	CLOSE,       // close request, it will immediately be changed to CLOSEWAIT or CLOSED
	CLOSEWAIT,   // waits for a worker to finish requested job, then will be closed
	CLOSED       // ready to be deleted
};

struct packetstruct {
	packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t *packet;
	std::unique_ptr<OutputBuffer> outputBuffer;

	packetstruct() : next(nullptr), startptr(nullptr), bytesleft(0), packet(nullptr) {
	}
};

class MessageSerializer;

struct csserventry {
	void* workerJobPool; // Job pool assigned to a given network worker thread

	uint8_t state;
	uint8_t mode;
	uint8_t fwdmode;

	int sock;
	int fwdsock; // forwarding socket for writing
	uint64_t connstart; // 'connect' start time in usec (for timeout and retry)
	uint8_t connretrycnt; // 'connect' retry counter
	NetworkAddress fwdServer; // the next server in write chain
	int32_t pdescpos;
	int32_t fwdpdescpos;
	uint32_t activity;
	uint8_t hdrbuff[PacketHeader::kSize];
	uint8_t fwdhdrbuff[PacketHeader::kSize];
	packetstruct inputpacket;
	uint8_t *fwdstartptr; // used for forwarding inputpacket data
	uint32_t fwdbytesleft; // used for forwarding inputpacket data
	packetstruct fwdinputpacket; // used for receiving status from fwdsocket
	std::vector<uint8_t> fwdinitpacket; // used only for write initialization
	packetstruct *outputhead, **outputtail;

	/* write */
	uint32_t wjobid;
	uint32_t wjobwriteid;
	std::set<uint32_t> partiallyCompletedWrites; // writeId's which:
	// * have been completed by our worker, but need ack from the next chunkserver from the chain
	// * have been acked by the next chunkserver from the chain, but are still being written by us

	/* read */
	uint32_t rjobid;
	uint8_t todocnt; // R (read finished + send finished)

	/* get blocks */
	uint32_t getBlocksJobId;
	uint16_t getBlocksJobResult;

	/* common for read and write but meaning is different !!! */
	void *rpacket;
	void *wpacket;

	uint8_t chunkisopen;
	uint64_t chunkid; // R+W
	uint32_t version; // R+W
	ChunkType chunkType; // R
	uint32_t offset; // R
	uint32_t size; // R
	MessageSerializer* messageSerializer; // R+W

	LOG_AVG_TYPE readOperationTimer;

	struct csserventry *next;

	csserventry(int socket, void* workerJobPool)
			: workerJobPool(workerJobPool),
			  state(IDLE),
			  mode(HEADER),
			  fwdmode(HEADER),
			  sock(socket),
			  fwdsock(-1),
			  connstart(0),
			  connretrycnt(0),
			  pdescpos(-1),
			  fwdpdescpos(-1),
			  activity(0),
			  fwdstartptr(NULL),
			  fwdbytesleft(0),
			  outputhead(nullptr),
			  outputtail(&outputhead),
			  wjobid(0),
			  wjobwriteid(0),
			  rjobid(0),
			  todocnt(0),
			  getBlocksJobId(0),
			  getBlocksJobResult(0),
			  rpacket(nullptr),
			  wpacket(nullptr),
			  chunkisopen(0),
			  chunkid(0),
			  version(0),
			  chunkType(ChunkType::getStandardChunkType()),
			  offset(0),
			  size(0),
			  messageSerializer(nullptr),
			  next(nullptr) {
		inputpacket.bytesleft = 8;
		inputpacket.startptr = hdrbuff;
		inputpacket.packet = NULL;
	}

	csserventry(const csserventry&) = delete;
	csserventry(csserventry&&) = default;
	csserventry& operator=(const csserventry&) = delete;
};

class NetworkWorkerThread {
public:
	NetworkWorkerThread(uint32_t nrOfBgjobsWorkers, uint32_t bgjobsCount);
	NetworkWorkerThread(const NetworkWorkerThread&) = delete;

	// main loop
	void operator()();
	void askForTermination();
	void addConnection(int newSocketFD);
	void* bgJobPool() {
		return bgJobPool_;
	}

	static std::atomic<bool> useSplice;

private:
	void preparePollFds();
	void servePoll() ;
	void terminate();

	std::atomic<bool> doTerminate;
	std::mutex csservheadLock;
	std::list<csserventry> csservEntries;

	void *bgJobPool_;
	int bgJobPoolWakeUpFd_;
	static const uint32_t JOB_FD_PDESC_POS = 1;
	std::vector<struct pollfd> pdesc;
	int notify_pipe[2];
};

