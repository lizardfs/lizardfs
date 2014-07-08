#include "common/platform.h"

#include <fcntl.h>
#include <iostream>
#include <boost/make_shared.hpp>
#include <polonaise/polonaise_constants.h>
#include <polonaise/Polonaise.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
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
 * It indicates both an operation which can't be performed (e.g. because of permission denied)
 * or a LizardFS error
 * \param type Status code
 * \return exception
 */
static Status makeStatus(StatusCode::type type) {
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
static Failure makeFailure(std::string message) {
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
		throw makeFailure("Unknown errno code: " + std::to_string(errNo));
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
		throw makeFailure("Incorrect int32_t value: " + std::to_string(value));
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
		throw makeFailure("Incorrect uint32_t value: " + std::to_string(value));
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
		throw makeFailure("Incorrect uint64_t value: " + std::to_string(value));
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

	flags |= polonaiseFlags & OpenFlags::kCreate    ? O_CREAT  : 0;
	flags |= polonaiseFlags & OpenFlags::kExclusive ? O_EXCL   : 0;
	flags |= polonaiseFlags & OpenFlags::kTrunc     ? O_TRUNC  : 0;
	flags |= polonaiseFlags & OpenFlags::kAppend    ? O_APPEND : 0;
	return flags;
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
		throw makeFailure("Context conversion failed: " + fail.message);
	}
}

/**
 * Convert Polonaise file type to a part of Unix file mode
 * \param typ file type
 * \throw Failure when file type is unknown
 * \return file mode with a file type filled
 */
static mode_t toModeFileType(const FileType::type type) throw(Failure) {
	mode_t result;
	switch (type) {
		case FileType::kDirectory:
			result = S_IFDIR;
			break;
		case FileType::kCharDevice:
			result = S_IFCHR;
			break;
		case FileType::kBlockDevice:
			result = S_IFBLK;
			break;
		case FileType::kRegular:
			result = S_IFREG;
			break;
		case FileType::kFifo:
			result = S_IFIFO;
			break;
		case FileType::kSymlink:
			result = S_IFLNK;
			break;
		case FileType::kSocket:
			result = S_IFSOCK;
			break;
		default:
			throw makeFailure("Unknown Polonaise file type: " + std::to_string(type));
	}
	return result;
}

/**
 * Prepare file mode using arguments given by Polonaise client
 * \param type file type (file, directory, socket etc.)
 * \param mode file mode
 * \throw Failure when file type is incorrect
 * \return Unix file mode
 */
static mode_t toLizardFsMode(const FileType::type& type, const Mode& mode) throw(Failure) {
	mode_t result = toModeFileType(type);
	result |= mode.setUid ? S_ISUID : 0;
	result |= mode.setGid ? S_ISGID : 0;
	result |= mode.sticky ? S_ISVTX : 0;
	result |= mode.ownerMask & AccessMask::kRead    ? S_IRUSR : 0;
	result |= mode.ownerMask & AccessMask::kWrite   ? S_IWUSR : 0;
	result |= mode.ownerMask & AccessMask::kExecute ? S_IXUSR : 0;
	result |= mode.groupMask & AccessMask::kRead    ? S_IRGRP : 0;
	result |= mode.groupMask & AccessMask::kWrite   ? S_IWGRP : 0;
	result |= mode.groupMask & AccessMask::kExecute ? S_IXGRP : 0;
	result |= mode.otherMask & AccessMask::kRead    ? S_IROTH : 0;
	result |= mode.otherMask & AccessMask::kWrite   ? S_IWOTH : 0;
	result |= mode.otherMask & AccessMask::kExecute ? S_IXOTH : 0;
	return result;
}

/**
 * Prepare LizardFS attributes set from Polonaise's ones.
 * The logic of the set is checked by a LizardFS call
 * \param set Polonaise attributes set
 * \return LizardFS one
 */
static int toLizardfsAttributesSet(int32_t set) {
	int result = 0;
	result |= (set & ToSet::kMode     ? LIZARDFS_SET_ATTR_MODE      : 0);
	result |= (set & ToSet::kUid      ? LIZARDFS_SET_ATTR_UID       : 0);
	result |= (set & ToSet::kGid      ? LIZARDFS_SET_ATTR_GID       : 0);
	result |= (set & ToSet::kSize     ? LIZARDFS_SET_ATTR_SIZE      : 0);
	result |= (set & ToSet::kAtime    ? LIZARDFS_SET_ATTR_ATIME     : 0);
	result |= (set & ToSet::kMtime    ? LIZARDFS_SET_ATTR_MTIME     : 0);
	result |= (set & ToSet::kAtimeNow ? LIZARDFS_SET_ATTR_ATIME_NOW : 0);
	result |= (set & ToSet::kMtimeNow ? LIZARDFS_SET_ATTR_MTIME_NOW : 0);
	return result;
}

/**
 * Convert file statistics from Polonaise type to C standard one
 * \param out Polonaise files stats
 * \param in standard file stats
 * \throw Failure when file type is unknown
 * \return result stats
 */
static struct stat toStructStat(const FileStat& fstat) throw(Failure) {
	struct stat result;
	result.st_mode = toLizardFsMode(fstat.type, fstat.mode);
	result.st_dev = fstat.dev;
	result.st_ino = fstat.inode;
	result.st_nlink = fstat.nlink;
	result.st_uid = fstat.uid;
	result.st_gid = fstat.gid;
	result.st_rdev = fstat.rdev;
	result.st_size = fstat.size;
	result.st_blksize = fstat.blockSize;
	result.st_blocks = fstat.blocks;
	result.st_atime = fstat.atime;
	result.st_mtime = fstat.mtime;
	result.st_ctime = fstat.ctime;
	return result;
}

/**
 * Convert file statistics from C standard type to a Polonaise one
 * \param in standard file stats
 * \throw Failure when file type is unknown
 * \return Polonaise files stats
 */
static FileStat toFileStat(const struct stat& in) throw(Failure) {
	FileStat reply;
	switch (in.st_mode & S_IFMT) {
		case S_IFDIR:
			reply.type = FileType::kDirectory;
			break;
		case S_IFCHR:
			reply.type = FileType::kCharDevice;
			break;
		case S_IFBLK:
			reply.type = FileType::kBlockDevice;
			break;
		case S_IFREG:
			reply.type = FileType::kRegular;
			break;
		case S_IFIFO:
			reply.type = FileType::kFifo;
			break;
		case S_IFLNK:
			reply.type = FileType::kSymlink;
			break;
		case S_IFSOCK:
			reply.type = FileType::kSocket;
			break;
		default:
			throw makeFailure("Unknown LizardFS file type: " + std::to_string(in.st_mode & S_IFMT));
	}
	reply.mode.setUid = in.st_mode & S_ISUID;
	reply.mode.setGid = in.st_mode & S_ISGID;
	reply.mode.sticky = in.st_mode & S_ISVTX;
	reply.mode.ownerMask = reply.mode.groupMask = reply.mode.otherMask = 0;
	reply.mode.ownerMask |= in.st_mode & S_IRUSR ? AccessMask::kRead    : 0;
	reply.mode.ownerMask |= in.st_mode & S_IWUSR ? AccessMask::kWrite   : 0;
	reply.mode.ownerMask |= in.st_mode & S_IXUSR ? AccessMask::kExecute : 0;
	reply.mode.groupMask |= in.st_mode & S_IRGRP ? AccessMask::kRead    : 0;
	reply.mode.groupMask |= in.st_mode & S_IWGRP ? AccessMask::kWrite   : 0;
	reply.mode.groupMask |= in.st_mode & S_IXGRP ? AccessMask::kExecute : 0;
	reply.mode.otherMask |= in.st_mode & S_IROTH ? AccessMask::kRead    : 0;
	reply.mode.otherMask |= in.st_mode & S_IWOTH ? AccessMask::kWrite   : 0;
	reply.mode.otherMask |= in.st_mode & S_IXOTH ? AccessMask::kExecute : 0;

	reply.dev = in.st_dev;
	reply.inode = in.st_ino;
	reply.nlink = in.st_nlink;
	reply.uid = in.st_uid;
	reply.gid = in.st_gid;
	reply.rdev = in.st_rdev;
	reply.size = in.st_size;
	reply.blockSize = in.st_blksize;
	reply.blocks = in.st_blocks;
	reply.atime = in.st_atime;
	reply.mtime = in.st_mtime;
	reply.ctime = in.st_ctime;
	return reply;
}

/**
 * Convert file's entry to be sent to Polonaise client
 * \param in LizardFS's entry
 * \throw Failure when file type couldn't be converted
 * \return Polonaise's entry
 */
static EntryReply toEntryReply(const LizardClient::EntryParam& in) throw(Failure) {
	if (in.attr_timeout < 0.0 || in.entry_timeout < 0.0) {
		throw makeFailure("Invalid timeout");
	}
	EntryReply result;
	result.inode = in.ino;
	result.generation = in.generation;
	result.attributes = toFileStat(in.attr);
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
			throw makeStatus(toStatusCode(ex.errNo));\
		} catch (Failure& ex) {\
			std::cerr << __FUNCTION__ << " failure: " << ex.message << std::endl;\
			throw makeFailure(std::string(__FUNCTION__) + ": " + ex.message);\
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
		_return = toEntryReply(entry);
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
		_return.attributes = toFileStat(reply.attr);
		_return.attributesTimeout = reply.attrTimeout;
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.setattr method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void setattr(AttributesReply& _return, const Context& context, const Inode inode,
			const FileStat& attributes, const int32_t toSet, const Descriptor descriptor) {
		OPERATION_PROLOG
		struct stat stats = toStructStat(attributes);
		LizardClient::AttrReply reply = LizardClient::setattr(
				toLizardFsContext(context),
				toUint64(inode),
				&stats,
				toLizardfsAttributesSet(toSet),
				getFileInfo(descriptor));
		_return.attributes = toFileStat(reply.attr);
		_return.attributesTimeout = reply.attrTimeout;
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.mknod method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void mknod(EntryReply& _return, const Context& context, const Inode parent,
			const std::string& name, const FileType::type type, const Mode& mode,
			const int32_t rdev) {
		OPERATION_PROLOG
		LizardClient::EntryParam reply = LizardClient::mknod(
				toLizardFsContext(context),
				toUint64(parent),
				name.c_str(),
				toLizardFsMode(type, mode),
				rdev);
		_return = toEntryReply(reply);
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.mkdir method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void mkdir(EntryReply& _return, const Context& context, const Inode parent,
			const std::string& name, const FileType::type type, const Mode& mode) {
		OPERATION_PROLOG
		LizardClient::EntryParam reply = LizardClient::mkdir(
				toLizardFsContext(context),
				toUint64(parent),
				name.c_str(),
				toLizardFsMode(type, mode));
		_return = toEntryReply(reply);
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.opendir method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	Descriptor opendir(const Context& context, const Inode inode) {
		OPERATION_PROLOG
		Descriptor descriptor = createDescriptor(0);
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
			throw makeFailure("Null descriptor");
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
			_return.back().attributes = toFileStat(entry.attr);
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
			throw makeFailure("Null descriptor");
		}
		LizardClient::releasedir(
				toLizardFsContext(context),
				toUint64(inode),
				getFileInfo(descriptor));
		removeDescriptor(descriptor);
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.rmdir method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void rmdir(const Context& context, const Inode parent, const std::string& name) {
		OPERATION_PROLOG
		LizardClient::rmdir(toLizardFsContext(context), toUint64(parent), name.c_str());
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.access method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void access(const Context& context, const Inode inode, const int32_t mask) {
		OPERATION_PROLOG
		int lizardFsMask = 0;
		lizardFsMask |= mask & AccessMask::kRead    ? MODE_MASK_R : 0;
		lizardFsMask |= mask & AccessMask::kWrite   ? MODE_MASK_W : 0;
		lizardFsMask |= mask & AccessMask::kExecute ? MODE_MASK_X : 0;
		LizardClient::access(
				toLizardFsContext(context),
				toUint64(inode),
				lizardFsMask);
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.create method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void create(CreateReply& _return, const Context& context, const Inode parent,
			const std::string& name, const Mode& mode, const int32_t flags) {
		OPERATION_PROLOG
		Descriptor descriptor = createDescriptor(flags);
		LizardClient::FileInfo* fi = getFileInfo(descriptor);
		LizardClient::EntryParam reply = LizardClient::create(
				toLizardFsContext(context),
				toUint64(parent),
				name.c_str(),
				toLizardFsMode(FileType::kRegular, mode),
				fi);
		_return.entry = toEntryReply(reply);
		_return.descriptor = descriptor;
		_return.directIo = fi->direct_io;
		_return.keepCache = fi->keep_cache;
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.open method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void open(OpenReply& _return, const Context& context, const Inode inode, const int32_t flags) {
		OPERATION_PROLOG
		Descriptor descriptor = createDescriptor(flags);
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
			throw makeFailure("Null descriptor");
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
	 * Implement Polonaise.write method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	int64_t write(const Context& context, const Inode inode, const int64_t offset,
			const int64_t size, const std::string& data, const Descriptor descriptor) {
		OPERATION_PROLOG
		if (descriptor == g_polonaise_constants.kNullDescriptor) {
			throw makeFailure("Null descriptor");
		}
		LizardClient::BytesWritten written = LizardClient::write(
				toLizardFsContext(context),
				toUint64(inode),
				data.data(),
				size,
				offset,
				getFileInfo(descriptor));
		return written;
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.fsync method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void fsync(const Context& context, const Inode inode, const bool syncOnlyData,
			const Descriptor descriptor) {
		OPERATION_PROLOG
		if (descriptor == g_polonaise_constants.kNullDescriptor) {
			throw makeFailure("Null descriptor");
		}
		LizardClient::fsync(
				toLizardFsContext(context),
				toUint64(inode),
				syncOnlyData,
				getFileInfo(descriptor));
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.flush method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void flush(const Context& context, const Inode inode, const Descriptor descriptor) {
		OPERATION_PROLOG
		if (descriptor == g_polonaise_constants.kNullDescriptor) {
			throw makeFailure("Null descriptor");
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
			throw makeFailure("Null descriptor");
		}
		LizardClient::release(toLizardFsContext(context), toUint64(inode), getFileInfo(descriptor));
		removeDescriptor(descriptor);
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

	/**
	 * Implement Polonaise.symlink method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void symlink(EntryReply& _return, const Context& context, const std::string& path,
				const Inode parent, const std::string& name) {
		OPERATION_PROLOG
		LizardClient::EntryParam entry = LizardClient::symlink(
				toLizardFsContext(context),
				path.c_str(),
				toUint64(parent),
				name.c_str());
		_return = toEntryReply(entry);
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.readlink method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void readlink(std::string& _return, const Context& context, const Inode inode) {
		OPERATION_PROLOG
		_return = LizardClient::readlink(toLizardFsContext(context), toUint64(inode));
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.link method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void link(EntryReply& _return, const Context& context, const Inode inode,
			const Inode newParent, const std::string& newName) {
		OPERATION_PROLOG
		_return = toEntryReply(LizardClient::link(
				toLizardFsContext(context),
				toUint64(inode),
				toUint64(newParent),
				newName.c_str()));
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.unlink method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void unlink(const Context& context, const Inode parent, const std::string& name) {
		OPERATION_PROLOG
		LizardClient::unlink(toLizardFsContext(context), toUint64(parent), name.c_str());
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.rename method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void rename(const Context& context, const Inode parent, const std::string& name,
			const Inode newParent, const std::string& newName) {
		OPERATION_PROLOG
		LizardClient::rename(
				toLizardFsContext(context),
				toUint64(parent),
				name.c_str(),
				toUint64(newParent),
				newName.c_str());
		OPERATION_EPILOG
	}

private:
	/**
	 * Create a new entry of opened file
	 * \param polonaiseFlags open flags
	 * \return descriptor
	 */
	Descriptor createDescriptor(int32_t polonaiseFlags) {
		std::lock_guard<std::mutex> lock(mutex_);
		Descriptor descriptor = ++lastDescriptor_;
		fileInfos_.insert({descriptor,
				LizardClient::FileInfo(toLizardFsFlags(polonaiseFlags), 0, 0, descriptor)});
		return descriptor;
	}

	/**
	 * Removes an entry of opened file
	 * \param descriptor descriptor to be deleted
	 * \throw Failure when descriptor doesn't exist
	 */
	void removeDescriptor(Descriptor descriptor) {
		std::lock_guard<std::mutex> lock(mutex_);
		auto it = fileInfos_.find(descriptor);
		if (it == fileInfos_.end()) {
			throw makeFailure("descriptor " + std::to_string(descriptor) + " not found");
		}
		fileInfos_.erase(it);
	}

	/**
	 * Get file information for LizardFS by a descriptor
	 * \param descriptor identifier of opened file
	 * \throw Failure when descriptor doesn't exist
	 * \return pointer to file information or NULL when null descriptor is given
	 */
	LizardClient::FileInfo* getFileInfo(Descriptor descriptor) {
		std::lock_guard<std::mutex> lock(mutex_);
		if (descriptor == g_polonaise_constants.kNullDescriptor) {
			return nullptr;
		}
		auto it = fileInfos_.find(descriptor);
		if (it == fileInfos_.end()) {
			throw makeFailure("descriptor " + std::to_string(descriptor) + " not found");
		}
		return &it->second;
	}

	/**
	 * Mutex for operations which modify fileInfos_ and lastDescriptor_
	 */
	std::mutex mutex_;

	/**
	 * Map for containing file information for each opened file
	 */
	std::map<Descriptor, LizardClient::FileInfo> fileInfos_;

	/**
	 * Last descriptor used
	 */
	Descriptor lastDescriptor_;
};

// Creates a TBufferedTransport with a read buffer of 512 KiB and write buffer size of 4KiB
class BigBufferedTransportFactory : public apache::thrift::transport::TTransportFactory {
public:
	static const uint32_t kReadBufferSize = 512 * 1024;
	static const uint32_t kWriteBufferSize = 4096;

	virtual boost::shared_ptr<apache::thrift::transport::TTransport> getTransport(
			boost::shared_ptr<apache::thrift::transport::TTransport> transport) {
		return boost::make_shared<apache::thrift::transport::TBufferedTransport>(
				transport, kReadBufferSize, kWriteBufferSize);
	}
};
const uint32_t BigBufferedTransportFactory::kReadBufferSize;
const uint32_t BigBufferedTransportFactory::kWriteBufferSize;

int main (int argc, char **argv) {
	uint32_t ioretries = 30;
	uint32_t cachesize_MB = 10;
	uint32_t reportreservedperiod = 60;
	if (argc < 4 || argc > 5) {
		std::cerr << "Usage: " << argv[0] <<
				" <master ip> <master port> <mountpoint> [<listen port>]\n" <<
				"    Default listen port is 9090" << std::endl;
		return 1;
	}

	// Initialize LizardFS client
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

	// Thrift server start
	int port = argc == 5 ? std::stoi(argv[4]) : 9090;
	using namespace ::apache::thrift;
	using namespace ::apache::thrift::transport;
	using namespace ::apache::thrift::server;
	boost::shared_ptr<PolonaiseHandler> handler(new PolonaiseHandler());
	boost::shared_ptr<TProcessor> processor(new PolonaiseProcessor(handler));
	boost::shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
	boost::shared_ptr<TTransportFactory> transportFactory(new BigBufferedTransportFactory());
	boost::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
	TThreadedServer server(processor, serverTransport, transportFactory, protocolFactory);
	server.serve();
	return 0;
}
