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

/**
 * Prepare a Polonaise's Status exception which informs the client that current operation
 * can't be performed in LizardFS.
 * It indicates denied permissions (e.g. on operation of access) or some error.
 * \param type Status code
 * \return exception
 */
static Status status(StatusCode::type type) {
	Status ex;
	ex.statusCode = type;
	return ex;
}

/**
 * Prepare a Polonaise's Failure exception which informs the client that arguments passed
 * to server are incorrect
 * \param message description of failure
 * \return exception
 */
static Failure failure(std::string message) {
	Failure ex;
	ex.message = std::move(message);
	return ex;
}

/**
 * Convert errno to Polonaise's StatusCode
 * \param errNo errno number from LizardFS client
 * \throw Failure when errno number is unknown
 * \return corresponding StatusCode for Polonaise client
 */
StatusCode::type toStatusCode(int errNo) throw(Failure) {
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

/**
 * Convert general Thrift integer type to int32_t
 * \param value input value
 * \throw Failure when given number exceeds returned type limits
 * \return value in int32_t
 */
static uint32_t toInt32(int64_t value) {
	if (value < INT32_MIN || value > INT32_MAX) {
		throw failure("Incorrect int32_t value: " + std::to_string(value));
	}
	return (int32_t)value;
}

/**
 * Convert general Thrift integer type to uint32_t
 * \param value input value
 * \throw Failure when given number exceeds returned type limits
 * \return value in uint32_t
 */
static uint32_t toUint32(int64_t value) {
	if (value < 0 || value > UINT32_MAX) {
		throw failure("Incorrect uint32_t value: " + std::to_string(value));
	}
	return (uint32_t)value;
}

/**
 * Convert general Thrift integer type to uint64_t
 * \param value input value
 * \throw Failure when given number exceeds returned type limits
 * \return value in uint64_t
 */
static uint64_t toUint64(int64_t value) {
	if (value < 0) {
		throw failure("Incorrect uint64_t value: " + std::to_string(value));
	}
	return (uint64_t)value;
}

/**
 * Convert flags from Polonaise client to LizardFS ones
 * \param polonaiseFlags flags in Polonaise format
 * \return flags in LizardFS format
 */
static int toLizardFsFlags(int32_t polonaiseFlags) {
	int flags = 0;
	if ((polonaiseFlags & OpenFlags::kRead) && !(polonaiseFlags & OpenFlags::kWrite)) {
		flags |= O_RDONLY;
	}
	else if (!(polonaiseFlags & OpenFlags::kRead) && (polonaiseFlags & OpenFlags::kWrite)) {
		flags |= O_WRONLY;
	}
	else if ((polonaiseFlags & OpenFlags::kRead) && (polonaiseFlags & OpenFlags::kWrite)) {
		flags |= O_RDWR;
	}

	if (polonaiseFlags & OpenFlags::kCreate) {
		flags |= O_CREAT;
	}
	if (polonaiseFlags & OpenFlags::kExclusive) {
		flags |= O_EXCL;
	}
	if (polonaiseFlags & OpenFlags::kTrunc) {
		flags |= O_TRUNC;
	}
	if (polonaiseFlags & OpenFlags::kAppend) {
		flags |= O_APPEND;
	}
	return flags;
}

/**
 * Convert access mask from Polonaise client to LizardFS one
 * \param polonaiseMask mask in Polonaise format
 * \return mask in LizardFS format
 */
static int32_t toLizardFsAccessMask(int32_t polonaiseMask) {
	int32_t mask = 0;
	if (polonaiseMask & AccessMask::kRead) {
		mask |= MODE_MASK_R;
	}
	if (polonaiseMask & AccessMask::kWrite) {
		mask |= MODE_MASK_W;
	}
	if (polonaiseMask & AccessMask::kExecute) {
		mask |= MODE_MASK_X;
	}
	return mask;
}

/**
 * Convert operation context to LizardFS format
 * \param ctx context in Polonaise format
 * \throw Failure when context couldn't be converted
 * \return converted context
 */
static LizardClient::Context toLizardFsContext(const Context& ctx) throw(Failure) {
	try {
		LizardClient::Context outCtx(toUint32(ctx.uid), toUint32(ctx.gid), toInt32(ctx.pid),
				toUint32(ctx.umask));
		return outCtx;
	} catch (Failure& fail) {
		throw failure("Context conversion failed: " + fail.message);
	}
}

/**
 * Convert file statistics from C standard type to a Polonaise one
 * \param in standard file stats
 * \throw Failure when file type is unknown
 * \return Polonaise files stats
 */
static FileStat toFileStat(const struct stat& in) throw(Failure) {
	FileStat result;
	switch (in.st_mode & S_IFMT) {
		case S_IFDIR:
			result.type = FileType::kDirectory;
			break;
		case S_IFCHR:
			result.type = FileType::kCharDevice;
			break;
		case S_IFBLK:
			result.type = FileType::kBlockDevice;
			break;
		case S_IFREG:
			result.type = FileType::kRegular;
			break;
		case S_IFIFO:
			result.type = FileType::kFifo;
			break;
		case S_IFLNK:
			result.type = FileType::kSymlink;
			break;
		case S_IFSOCK:
			result.type = FileType::kSocket;
			break;
		default:
			throw failure("Unknown file type: " + std::to_string(in.st_mode & S_IFMT));
	}

	result.dev = in.st_dev;
	result.inode = in.st_ino;
	result.nlink = in.st_nlink;
	result.mode = in.st_mode;
	result.uid = in.st_uid;
	result.gid = in.st_gid;
	result.rdev = in.st_rdev;
	result.size = in.st_size;
	result.blockSize = in.st_blksize;
	result.blocks = in.st_blocks;
	result.atime = in.st_atime;
	result.mtime = in.st_mtime;
	result.ctime = in.st_ctime;
	return result;
}

/**
 * Convert file's entry to be sent to Polonaise client
 * \param in LizardFS's entry
 * \throw Failure when file type couldn't be converted
 * \return Polonaise's entry
 */
static EntryReply toEntryReply(const LizardClient::EntryParam& in) throw(Failure) {
	if (in.attr_timeout < 0.0 || in.entry_timeout < 0.0) {
		throw failure("Invalid timeout");
	}
	EntryReply result;
	result.inode = in.ino;
	result.generation = in.generation;
	result.attributes = std::move(toFileStat(in.attr));
	result.attributesTimeout = in.attr_timeout;
	result.entryTimeout = in.entry_timeout;
	return result;
}

/**
 * Begin a general handling of exceptions
 */
#define OPERATION_PROLOG\
	try {

/**
 * End a general handling of exceptions
 */
#define OPERATION_EPILOG\
		} catch (LizardClient::RequestException& ex) {\
			throw status(toStatusCode(ex.errNo));\
		} catch (Failure& ex) {\
			std::cerr << __FUNCTION__ << " failure: " << ex.message << std::endl;\
			throw failure(std::string(__FUNCTION__) + ": " + ex.message);\
		}

/**
 * Polonaise interface which operates on a LizardFS client
 */
class PolonaiseHandler : virtual public PolonaiseIf {
public:
	/**
	 * Constructor
	 */
	PolonaiseHandler() : lastDescriptor_(g_polonaise_constants.kNullDescriptor) {}

	/**
	 * Implement Polonaise.initSession method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	SessionId initSession() {
		OPERATION_PROLOG
		return 0;
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.lookup method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void lookup(EntryReply& _return, const Context& context, const Inode inode,
			const std::string& name) {
		OPERATION_PROLOG
		LizardClient::EntryParam entry = LizardClient::lookup(
				toLizardFsContext(context),
				toUint64(inode),
				name.c_str());
		_return = std::move(toEntryReply(entry));
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.getattr method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void getattr(AttributesReply& _return, const Context& context, const Inode inode,
			const Descriptor descriptor) {
		OPERATION_PROLOG
		LizardClient::AttrReply reply = LizardClient::getattr(
				toLizardFsContext(context),
				toUint64(inode),
				getFileInfo(descriptor));
		_return.attributes = std::move(toFileStat(reply.attr));
		_return.attributesTimeout = reply.attrTimeout;
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.opendir method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	Descriptor opendir(const Context& context, const Inode inode) {
		OPERATION_PROLOG
		Descriptor descriptor = ++lastDescriptor_;
		fileInfos_.insert({descriptor, LizardClient::FileInfo(0, 0, 0, 0)});
		LizardClient::opendir(toLizardFsContext(context), toUint32(inode), getFileInfo(descriptor));
		return descriptor;
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.readdir method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void readdir(std::vector<DirectoryEntry> & _return, const Context& context,
			const Inode inode, const int64_t firstEntryOffset,
			const int64_t maxNumberOfEntries, const Descriptor descriptor) {
		OPERATION_PROLOG
		if (descriptor == g_polonaise_constants.kNullDescriptor) {
			throw failure("Null descriptor");
		}
		std::vector<LizardClient::DirEntry> entries = LizardClient::readdir(
				toLizardFsContext(context),
				toUint64(inode),
				firstEntryOffset,
				toUint64(maxNumberOfEntries),
				getFileInfo(descriptor));

		_return.reserve(entries.size());
		for (LizardClient::DirEntry& entry : entries) {
			_return.push_back({});
			_return.back().name = std::move(entry.name);
			_return.back().attributes = std::move(toFileStat(entry.attr));
			_return.back().nextEntryOffset = entry.nextEntryOffset;
		}
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.releasedir method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void releasedir(const Context& context, const Inode inode, const Descriptor descriptor) {
		OPERATION_PROLOG
		if (descriptor == g_polonaise_constants.kNullDescriptor) {
			throw failure("Null descriptor");
		}
		LizardClient::releasedir(toLizardFsContext(context), toUint64(inode), getFileInfo(descriptor));
		fileInfos_.erase(descriptor);
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.access method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void access(const Context& context, const Inode inode, const int32_t mask) {
		OPERATION_PROLOG
		LizardClient::access(toLizardFsContext(context), toUint64(inode), toLizardFsAccessMask(mask));
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.open method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void open(OpenReply& _return, const Context& context, const Inode inode, const int32_t flags) {
		OPERATION_PROLOG
		Descriptor descriptor = ++lastDescriptor_;
		fileInfos_.insert({descriptor, LizardClient::FileInfo(toLizardFsFlags(flags), 0, 0, 0)});
		LizardClient::FileInfo* fi = getFileInfo(descriptor);
		LizardClient::open(toLizardFsContext(context), toUint64(inode), fi);
		_return.descriptor = descriptor;
		_return.directIo = fi->direct_io;
		_return.keepCache = fi->keep_cache;
		_return.nonSeekable = 0;
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.read method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void read(std::string& _return, const Context& context, const Inode inode,
			const int64_t offset, const int64_t size, const Descriptor descriptor) {
		OPERATION_PROLOG
		if (descriptor == g_polonaise_constants.kNullDescriptor) {
			throw failure("Null descriptor");
		}
		std::vector<uint8_t> buffer = LizardClient::read(
				toLizardFsContext(context),
				toUint64(inode),
				size,
				offset,
				getFileInfo(descriptor));
		_return.assign(reinterpret_cast<const char*>(buffer.data()), buffer.size());
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.flush method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void flush(const Context& context, const Inode inode, const Descriptor descriptor) {
		OPERATION_PROLOG
		if (descriptor == g_polonaise_constants.kNullDescriptor) {
			throw failure("Null descriptor");
		}
		LizardClient::flush(toLizardFsContext(context), toUint64(inode), getFileInfo(descriptor));
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.release method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void release(const Context& context, const Inode inode, const Descriptor descriptor) {
		OPERATION_PROLOG
		if (descriptor == g_polonaise_constants.kNullDescriptor) {
			throw failure("Null descriptor");
		}
		LizardClient::release(toLizardFsContext(context), toUint64(inode), getFileInfo(descriptor));
		fileInfos_.erase(descriptor);
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.statfs method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void statfs(StatFsReply& _return, const Context& context, const Inode inode) {
		OPERATION_PROLOG
		struct statvfs sv = LizardClient::statfs(toLizardFsContext(context), toUint64(inode));
		_return.filesystemId = sv.f_fsid;
		_return.maxNameLength = sv.f_namemax;
		_return.blockSize = sv.f_bsize;
		_return.totalBlocks = sv.f_blocks;
		_return.freeBlocks = sv.f_bfree;
		_return.availableBlocks = sv.f_bavail;
		_return.totalFiles = sv.f_files;
		_return.freeFiles = sv.f_ffree;
		_return.availableFiles = sv.f_favail;
		OPERATION_EPILOG
	}

private:
	/**
	 * Get file information for LizardFS by a descriptor
	 * \param descriptor identifier of opened file
	 * \throw Failure when descriptor doesn't exist
	 * \return pointer to file information or NULL when null descriptor is given
	 */
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

	/**
	 * Map for containing file information for each opened file
	 */
	std::map<Descriptor, LizardClient::FileInfo> fileInfos_;

	/**
	 * Last descriptor used
	 */
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
	IoLimitsConfigLoader loader;
	gMountLimiter().loadConfiguration(loader);
	csdb_init();
	read_data_init(ioretries);
	write_data_init(cachesize_MB * 1024 * 1024, ioretries);
	LizardClient::init(1, 1, 0.0, 0.0, 0.0, 0, 0, true);

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
