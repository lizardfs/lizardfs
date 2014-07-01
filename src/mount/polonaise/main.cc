#include "common/platform.h"

#include <fcntl.h>
#include <iostream>

#include <polonaise/polonaise_constants.h>
#include <polonaise/Polonaise.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

#include "common/crc.h"
#include "mount/csdb.h"
#include "mount/g_io_limiters.h"
#include "mount/lizard_client.h"
#include "mount/mastercomm.h"
#include "mount/masterproxy.h"
#include "mount/readdata.h"
#include "mount/symlinkcache.h"
#include "mount/writedata.h"

using namespace ::polonaise;

static Status status(StatusCode::type etype) {
	Status ex;
	ex.statusCode = etype;
	return ex;
}

static Failure failure(std::string message) {
	Failure ex;
	ex.message = message;
	return ex;
}

StatusCode::type errnoToStatusCode(int errNo) {
	static std::map<int, StatusCode::type> statuses = {
			{ E2BIG, StatusCode::kE2BIG },
			{ EACCES, StatusCode::kEACCES },
			{ EAGAIN, StatusCode::kEAGAIN },
			{ EBADF, StatusCode::kEBADF },
			{ EBUSY, StatusCode::kEBUSY },
			{ ECHILD, StatusCode::kECHILD },
			{ EDOM, StatusCode::kEDOM },
			{ EEXIST, StatusCode::kEEXIST },
			{ EFAULT, StatusCode::kEFAULT },
			{ EFBIG, StatusCode::kEFBIG },
			{ EINTR, StatusCode::kEINTR },
			{ EINVAL, StatusCode::kEINVAL },
			{ EIO, StatusCode::kEIO },
			{ EISDIR, StatusCode::kEISDIR },
			{ EMFILE, StatusCode::kEMFILE },
			{ EMLINK, StatusCode::kEMLINK },
			{ ENAMETOOLONG, StatusCode::kENAMETOOLONG },
			{ ENFILE, StatusCode::kENFILE },
			{ ENODATA, StatusCode::kENODATA },
			{ ENODEV, StatusCode::kENODEV },
			{ ENOENT, StatusCode::kENOENT },
			{ ENOEXEC, StatusCode::kENOEXEC },
			{ ENOMEM, StatusCode::kENOMEM },
			{ ENOSPC, StatusCode::kENOSPC },
			{ ENOSYS, StatusCode::kENOSYS },
			{ ENOTBLK, StatusCode::kENOTBLK },
			{ ENOTDIR, StatusCode::kENOTDIR },
			{ ENOTEMPTY, StatusCode::kENOTEMPTY },
			{ ENOTTY, StatusCode::kENOTTY },
			{ ENXIO, StatusCode::kENXIO },
			{ EPERM, StatusCode::kEPERM },
			{ EPIPE, StatusCode::kEPIPE },
			{ ERANGE, StatusCode::kERANGE },
			{ EROFS, StatusCode::kEROFS },
			{ ESPIPE, StatusCode::kESPIPE },
			{ ESRCH, StatusCode::kESRCH },
			{ ETIMEDOUT, StatusCode::kETIMEDOUT },
			{ ETXTBSY, StatusCode::kETXTBSY },
			{ EXDEV, StatusCode::kEXDEV },
	};

	auto it = statuses.find(errNo);
	if (it == statuses.end()) {
		throw failure("Unknown errno code: " + std::to_string(errNo));
	}
	return it->second;
}

#define OPERATION_PROLOG\
	printf("%s\n", __FUNCTION__);\
	try {

#define OPERATION_EPILOG\
		} catch (LizardClient::RequestException& ex) {\
			throw status(errnoToStatusCode(ex.errNo));\
		} catch (Failure& ex) {\
			std::cerr << __FUNCTION__ << " failure: " << ex.message << std::endl;\
			throw failure(std::string(__FUNCTION__) + ": " + ex.message);\
		}

static uint32_t toInt32(int64_t value) {
	if (value < INT32_MIN || value > INT32_MAX) {
		throw failure("Incorrect int32_t value: " + std::to_string(value));
	}
	return (int32_t)value;
}

static uint32_t toUint32(int64_t value) {
	if (value < 0 || value > UINT32_MAX) {
		throw failure("Incorrect uint32_t value: " + std::to_string(value));
	}
	return (uint32_t)value;
}

static uint64_t toUint64(int64_t value) {
	if (value < 0) {
		throw failure("Incorrect uint64_t value: " + std::to_string(value));
	}
	return (uint64_t)value;
}

/* For future use
static int convertFlags(int32_t flagsFromClient) {
	int flags = 0;
	if ((flagsFromClient & OpenFlags::kRead) && !(flagsFromClient & OpenFlags::kWrite)) {
		flags |= O_RDONLY;
	}
	else if (!(flagsFromClient & OpenFlags::kRead) && (flagsFromClient & OpenFlags::kWrite)) {
		flags |= O_WRONLY;
	}
	else if ((flagsFromClient & OpenFlags::kRead) && (flagsFromClient & OpenFlags::kWrite)) {
		flags |= O_RDWR;
	}

	if (flagsFromClient & OpenFlags::kCreate) {
		flags |= O_CREAT;
	}
	if (flagsFromClient & OpenFlags::kExclusive) {
		flags |= O_EXCL;
	}
	if (flagsFromClient & OpenFlags::kTrunc) {
		flags |= O_TRUNC;
	}
	if (flagsFromClient & OpenFlags::kAppend) {
		flags |= O_APPEND;
	}
	return flags;
}
*/

static void convertStat(const struct stat& in, FileStat& out) throw(Failure) {
	switch (in.st_mode & S_IFMT) {
		case S_IFDIR:
			out.type = FileType::kDirectory;
			break;
		case S_IFCHR:
			out.type = FileType::kCharDevice;
			break;
		case S_IFBLK:
			out.type = FileType::kBlockDevice;
			break;
		case S_IFREG:
			out.type = FileType::kRegular;
			break;
		case S_IFIFO:
			out.type = FileType::kFifo;
			break;
		case S_IFLNK:
			out.type = FileType::kSymlink;
			break;
		case S_IFSOCK:
			out.type = FileType::kSocket;
			break;
		default:
			throw failure("Unknown inode type: " + std::to_string(in.st_mode & S_IFMT));
	}

	out.dev = in.st_dev;
	out.inode = in.st_ino;
	out.nlink = in.st_nlink;
	out.mode = in.st_mode;
	out.uid = in.st_uid;
	out.gid = in.st_gid;
	out.rdev = in.st_rdev;
	out.size = in.st_size;
	out.blockSize = in.st_blksize;
	out.blocks = in.st_blocks;
	out.atime = in.st_atime;
	out.mtime = in.st_mtime;
	out.ctime = in.st_ctime;
}

static void convertEntry(const LizardClient::EntryParam& in, EntryReply& out) throw(Failure) {
	if (in.attr_timeout < 0.0 || in.entry_timeout < 0.0) {
		throw failure("Invalid timeout");
	}
	out.inode = in.ino;
	out.generation = in.generation;
	convertStat(in.attr, out.attributes);
	out.attributesTimeout = in.attr_timeout;
	out.entryTimeout = in.entry_timeout;
}

static LizardClient::Context convertContext(const Context& ctx) throw(Failure) {
	try {
		LizardClient::Context outCtx(toUint32(ctx.uid), toUint32(ctx.gid), toInt32(ctx.pid),
				toUint32(ctx.umask));
		return outCtx;
	} catch (Failure& fail) {
		throw failure("Context conversion failed: " + fail.message);
	}
}

/**
 * TODO Comments everywhere
 */
class PolonaiseHandler : virtual public PolonaiseIf {
public:
	PolonaiseHandler() : lastDescriptor_(g_polonaise_constants.kNullDescriptor) {}

	SessionId initSession() {
		OPERATION_PROLOG
		return 0;
		OPERATION_EPILOG
	}

	void lookup(EntryReply& _return, const Context& context, const Inode inode,
			const std::string& name) {
		OPERATION_PROLOG
		LizardClient::EntryParam entry = LizardClient::lookup(
				convertContext(context),
				toUint64(inode),
				name.c_str());
		convertEntry(entry, _return);
		OPERATION_EPILOG
	}

	void getattr(AttributesReply& _return, const Context& context, const Inode inode,
			const Descriptor descriptor) {
		OPERATION_PROLOG
		LizardClient::AttrReply reply = LizardClient::getattr(
				convertContext(context),
				toUint64(inode),
				getFileInfo(descriptor));
		convertStat(reply.attr, _return.attributes);
		_return.attributesTimeout = reply.attrTimeout;
		OPERATION_EPILOG
	}

	Descriptor opendir(const Context& context, const Inode inode) {
		OPERATION_PROLOG
		Descriptor descriptor = ++lastDescriptor_;
		fileInfos_.insert({descriptor, LizardClient::FileInfo(0, 0, 0, 0)});
		LizardClient::FileInfo* fileInfo = getFileInfo(descriptor);
		LizardClient::opendir(convertContext(context), toUint32(inode), fileInfo);
		return descriptor;
		OPERATION_EPILOG
	}

	void readdir(std::vector<DirectoryEntry> & _return, const Context& context,
			const Inode inode, const int64_t firstEntryOffset,
			const int64_t maxNumberOfEntries, const Descriptor descriptor) {
		OPERATION_PROLOG
		if (descriptor == g_polonaise_constants.kNullDescriptor) {
			throw failure("Null descriptor");
		}
		std::vector<LizardClient::DirEntry> entries = LizardClient::readdir(
				convertContext(context),
				toUint64(inode),
				firstEntryOffset,
				toUint64(maxNumberOfEntries),
				getFileInfo(descriptor));

		_return.reserve(entries.size());
		for (LizardClient::DirEntry& entry : entries) {
			_return.push_back({});
			_return.back().name = std::move(entry.name);
			convertStat(entry.attr, _return.back().attributes);
			_return.back().nextEntryOffset = entry.nextEntryOffset;
		}
		OPERATION_EPILOG
	}

	void releasedir(const Context& context, const Inode inode, const Descriptor descriptor) {
		OPERATION_PROLOG
		if (descriptor == g_polonaise_constants.kNullDescriptor) {
			throw failure("Null descriptor");
		}
		LizardClient::releasedir(convertContext(context), toUint64(inode), getFileInfo(descriptor));
		fileInfos_.erase(descriptor);
		OPERATION_EPILOG
	}

private:
	LizardClient::FileInfo* getFileInfo(Descriptor descriptor) {
		if (descriptor == g_polonaise_constants.kNullDescriptor) {
			return nullptr;
		}
		auto it = fileInfos_.find(descriptor);
		if (it == fileInfos_.end()) {
			throw failure("descriptor " + std::to_string(descriptor) + " not found");
		}
		return &it->second;
	}

	std::map<Descriptor, LizardClient::FileInfo> fileInfos_;
	Descriptor lastDescriptor_;
};

int main (int argc, char **argv) {
	// TODO: Proper options passing
	uint32_t ioretries = 30;
	uint32_t cachesize_MB = 10;
	uint32_t reportreservedperiod = 60;
	if (argc != 4) {
		std::cerr << "Usage: " << argv[0] << " <master ip> <master port> <mountpoint>" << std::endl;
		return 1;
	}
	strerr_init();
	mycrc32_init();
	if (fs_init_master_connection(nullptr, argv[1], argv[2], 0, argv[3], "/", nullptr,
			0, 0, ioretries, reportreservedperiod) < 0) {
		std::cerr << "Can't initialize connection with master server" << std::endl;
		return 2;
	}
	symlink_cache_init();
	gGlobalIoLimiter();
	fs_init_threads(ioretries);
	masterproxy_init();
	gLocalIoLimiter();
	csdb_init();
	read_data_init(ioretries);
	write_data_init(cachesize_MB * 1024 * 1024, ioretries);
	LizardClient::init(0, 1, 0.0, 0.0, 0.0, 0, 0, true);

	int port = 9090;
	using namespace ::apache::thrift;
	using namespace ::apache::thrift::transport;
	using namespace ::apache::thrift::server;
	boost::shared_ptr<PolonaiseHandler> handler(new PolonaiseHandler());
	boost::shared_ptr<TProcessor> processor(new PolonaiseProcessor(handler));
	boost::shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
	boost::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
	boost::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

	TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
	server.serve();
	return 0;
}
