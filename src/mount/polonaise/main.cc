/*
   Copyright 2013-2014 EditShare, 2013-2017 Skytechnology sp. z o.o.

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

#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <iostream>
#include <boost/make_shared.hpp>
#include <polonaise/polonaise_constants.h>
#include <polonaise/Polonaise.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TPipeServer.h>
#include <thrift/transport/TServerSocket.h>

#include "common/crc.h"
#include "common/slogger.h"
#include "common/sockets.h"
#include "mount/errno_defs.h"
#include "mount/g_io_limiters.h"
#include "mount/lizard_client.h"
#include "mount/mastercomm.h"
#include "mount/masterproxy.h"
#include "mount/readdata.h"
#include "mount/symlinkcache.h"
#include "mount/writedata.h"
#include "mount/polonaise/options.h"
#include "mount/polonaise/setup.h"

#include "mount/stat_defs.h" // !!! This must be the last include. Do not move !!!

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
StatusCode::type toStatusCode(int errNo) {
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
			{ EDQUOT, StatusCode::kEDQUOT },
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
static LizardClient::Context toLizardFsContext(const Context& ctx) {
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
static mode_t toModeFileType(const FileType::type type) {
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
static mode_t toLizardFsMode(const FileType::type& type, const Mode& mode) {
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
static struct stat toStructStat(const FileStat& fstat) {
	struct stat result;
	result.st_mode = toLizardFsMode(fstat.type, fstat.mode);
	result.st_dev = fstat.dev;
	result.st_ino = fstat.inode;
	result.st_nlink = fstat.nlink;
	result.st_uid = fstat.uid;
	result.st_gid = fstat.gid;
	result.st_rdev = fstat.rdev;
	result.st_size = fstat.size;
#ifdef LIZARDFS_HAVE_STRUCT_STAT_ST_BLKSIZE
	result.st_blksize = fstat.blockSize;
#endif
#ifdef LIZARDFS_HAVE_STRUCT_STAT_ST_BLOCKS
	result.st_blocks = fstat.blocks;
#endif
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
static FileStat toFileStat(const struct stat& in) {
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
#ifdef LIZARDFS_HAVE_STRUCT_STAT_ST_BLKSIZE
	reply.blockSize = in.st_blksize;
#endif
#ifdef LIZARDFS_HAVE_STRUCT_STAT_ST_BLOCKS
	reply.blocks = in.st_blocks;
#endif
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
static EntryReply toEntryReply(const LizardClient::EntryParam& in) {
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
			lzfs_pretty_syslog(LOG_ERR, "%s Failure: %s", __FUNCTION__, ex.message.c_str());\
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
	void readdir(std::vector<polonaise::DirectoryEntry> & _return, const Context& context,
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

		if (LizardClient::isSpecialInode(inode)) {
			std::vector<uint8_t> buffer = LizardClient::read_special_inode(
					toLizardFsContext(context),
					toUint64(inode),
					size,
					offset,
					getFileInfo(descriptor));
			_return.assign(reinterpret_cast<const char*>(buffer.data()), buffer.size());
		} else {
			auto result = LizardClient::read(
					toLizardFsContext(context),
					toUint64(inode),
					size,
					offset,
					getFileInfo(descriptor));
			small_vector<std::pair<void *, std::size_t>, 8> reply;
			result.toIoVec(reply, offset, size);
			for (const auto &iov : reply) {
				const char *base = (const char *)iov.first;
				_return.append(base, base + iov.second);
			}
		}
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

	/**
	 * Implement Polonaise.getxattr method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void getxattr(std::string& _return, const Context& context, const Inode inode,
		      const std::string& name, const int64_t size) {
		OPERATION_PROLOG
		uint32_t position = 0;
		auto a = LizardClient::getxattr(toLizardFsContext(context), toUint64(inode),
						name.c_str(), size, position);
		if (size == 0) {
			a = LizardClient::getxattr(toLizardFsContext(context), toUint64(inode),
						   name.c_str(),  a.valueLength, position);
		}
		_return.assign(a.valueBuffer.begin(), a.valueBuffer.end());
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.setxattr method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void setxattr(const Context& context, const Inode inode, const std::string& name,
		      const std::string& value, const int64_t size, const int32_t flags) {
		OPERATION_PROLOG
		uint32_t position = 0;
		LizardClient::setxattr(toLizardFsContext(context), toUint64(inode), name.c_str(), value.c_str(),
				       size, flags, position);
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.listxattr method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void listxattr(std::string& _return, const Context& context, const Inode inode,
		       const int64_t size) {
		OPERATION_PROLOG
		auto a = LizardClient::listxattr(toLizardFsContext(context), toUint64(inode), size);
		if (size == 0) {
			a = LizardClient::listxattr(toLizardFsContext(context), toUint64(inode), a.valueLength);
		}
		_return.assign(a.valueBuffer.begin(), a.valueBuffer.end());
		OPERATION_EPILOG
	}

	/**
	 * Implement Polonaise.removexattr method
	 * \note for more information, see the protocol definition in Polonaise sources
	 */
	void removexattr(const Context& context, const Inode inode, const std::string& name) {
		OPERATION_PROLOG
		LizardClient::removexattr(toLizardFsContext(context), toUint64(inode), name.c_str());
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
				LizardClient::FileInfo(toLizardFsFlags(polonaiseFlags), 0, 0, descriptor, 0)});
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

static std::unique_ptr<apache::thrift::server::TThreadedServer> gServer;
static sig_atomic_t gTerminated = 0;

void termhandle(int) {
	gTerminated = 1;
	if (gServer) {
		gServer->stop();
	}
}

#ifndef _WIN32
bool daemonize() {
	pid_t pid;

	pid = fork();
	if (pid) {
		if (pid > 0) {
			exit(0);
		}

		lzfs_pretty_syslog(LOG_ERR, "First fork failed: %s", strerror(errno));
		return false ;
	}

	/* setsid() return value is ignored, because behaviour is the same regardless of
	 * its result.
	 */
	setsid();
	int r = chdir("/");
	if (r < 0) {
		lzfs_pretty_syslog(LOG_ERR, "Change directory failed: %s", strerror(errno));
	}
	umask(0);

	pid = fork();
	if (pid) {
		if (pid > 0) {
			exit(0);
		}

		lzfs_pretty_syslog(LOG_ERR, "Second fork failed: %s", strerror(errno));
		return false;
	}

	close(0);
	close(1);
	close(2);

	if (open("/dev/null", O_RDONLY) < 0) {
		lzfs_pretty_syslog(LOG_ERR, "Unable to open /dev/null: %s", strerror(errno));
		return false;
	}
	if (open("/dev/null", O_WRONLY) < 0) {
		lzfs_pretty_syslog(LOG_ERR, "Unable to open /dev/null: %s", strerror(errno));
		return false;
	}
	if (dup(1) < 0) {
		lzfs_pretty_syslog(LOG_ERR, "Unable to duplicate stdout descriptor: %s", strerror(errno));
		return false;
	}

	return true;
}
#endif

int main (int argc, char **argv) {
	bool userwlock = true;
	auto writeworkers = 30;
	auto writewindowsize = 15;
	auto chunkserverrtt = 200;
	auto chunkserverconnectreadto = 2000;
	auto chunkserverwavereadto = 500;
	auto chunkservertotalreadto = 2000;
	auto cacheexpirationtime = 500;
	auto readaheadmaxwindowsize = 4096;
	bool prefetchFullXorStripes = true;
	auto bandwidthOveruse = 1.25;
	auto chunkserverwriteto = 5000;
	auto cacheperinodepercentage = 25;
	parse_command_line(argc, argv, gSetup);

#ifndef _WIN32
	struct sigaction sa;
	sa.sa_flags = 0;
	sigemptyset(&sa.sa_mask);
	sa.sa_handler = termhandle;
	sigaction(SIGTERM, &sa, nullptr);
	sigaction(SIGINT, &sa, nullptr);
#if defined(SIGUSR1) && defined(ENABLE_EXIT_ON_USR1)
	sigaction(SIGUSR1, &sa, nullptr);
#endif

	openlog("lizardfs-polonaise-server", 0, LOG_DAEMON);

	// Daemonize if needed
	if (gSetup.make_daemon && !daemonize()) {
		lzfs_pretty_syslog(LOG_ERR, "Unable to daemonize lizardfs-polonaise-server");
		return EXIT_FAILURE;
	}
#endif

	setvbuf(stdout, NULL, _IONBF, 0);
	setvbuf(stderr, NULL, _IONBF, 0);

	// Initialize LizardFS client
	socketinit();
	strerr_init();
	mycrc32_init();
	if (fs_init_master_connection(nullptr,
				gSetup.master_host.c_str(), gSetup.master_port.c_str(),
				0, gSetup.mountpoint.c_str(), gSetup.subfolder.c_str(), nullptr,
				gSetup.forget_password, 0, gSetup.io_retries, gSetup.report_reserved_period) < 0) {
		std::cerr << "Can't initialize connection with master server" << std::endl;
		lzfs_pretty_syslog(LOG_ERR, "Can't initialize connection with master server");
		return 2;
	}
	symlink_cache_init();
	gGlobalIoLimiter();
	fs_init_threads(gSetup.io_retries);
	masterproxy_init();
	gLocalIoLimiter();
	IoLimitsConfigLoader loader;
	gMountLimiter().loadConfiguration(loader);
	read_data_init(gSetup.io_retries,
			chunkserverrtt,
			chunkserverconnectreadto,
			chunkserverwavereadto,
			chunkservertotalreadto,
			cacheexpirationtime,
			readaheadmaxwindowsize,
			prefetchFullXorStripes,
			bandwidthOveruse);
	write_data_init(gSetup.write_buffer_size, gSetup.io_retries, writeworkers,
			writewindowsize, chunkserverwriteto, cacheperinodepercentage);
	LizardClient::init(gSetup.debug, true, gSetup.direntry_cache_timeout, gSetup.direntry_cache_size,
			gSetup.entry_cache_timeout, gSetup.attr_cache_timeout,
			!gSetup.no_mkdir_copy_sgid, gSetup.sugid_clear_mode, gSetup.enable_acl, userwlock, 0, 0);

	// Thrift server start
	using namespace ::apache::thrift;
	using namespace ::apache::thrift::transport;
	using namespace ::apache::thrift::server;
	boost::shared_ptr<PolonaiseHandler> handler(new PolonaiseHandler());
	boost::shared_ptr<TProcessor> processor(new PolonaiseProcessor(handler));
	boost::shared_ptr<TTransportFactory> transportFactory(new BigBufferedTransportFactory());
	boost::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
	boost::shared_ptr<TServerTransport> serverTransport;

#ifdef _WIN32
	if (gSetup.bind_port > 0) {
		serverTransport.reset(new TServerSocket(gSetup.bind_port));
	} else {
		static const int kPipeBufferSize = 128 * 1024;
		serverTransport.reset(new TPipeServer(gSetup.pipe_name, kPipeBufferSize));
	}
#else
	serverTransport.reset(new TServerSocket(gSetup.bind_port));
#endif

	gServer.reset(new TThreadedServer(processor, serverTransport, transportFactory, protocolFactory));

	if (gTerminated == 0) {
		gServer->serve();
	}

	write_data_term();
	read_data_term();
	masterproxy_term();
	fs_term();
	symlink_cache_term();
	socketrelease();

	return 0;
}
