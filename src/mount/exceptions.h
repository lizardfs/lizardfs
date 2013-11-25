#ifndef LIZARDFS_MOUNT_EXCEPTIONS_H_
#define LIZARDFS_MOUNT_EXCEPTIONS_H_

#include "common/chunk_type.h"
#include "common/exceptions.h"

LIZARDFS_CREATE_EXCEPTION_CLASS(ReadException, Exception);
LIZARDFS_CREATE_EXCEPTION_CLASS(RecoverableReadException, ReadException);
LIZARDFS_CREATE_EXCEPTION_CLASS(UnrecoverableReadException, ReadException);
LIZARDFS_CREATE_EXCEPTION_CLASS(NoValidCopiesReadException, RecoverableReadException);

LIZARDFS_CREATE_EXCEPTION_CLASS(WriteException, Exception);
LIZARDFS_CREATE_EXCEPTION_CLASS(RecoverableWriteException, WriteException);
LIZARDFS_CREATE_EXCEPTION_CLASS(UnrecoverableWriteException, WriteException);
LIZARDFS_CREATE_EXCEPTION_CLASS(NoValidCopiesWriteException, RecoverableWriteException);

class ChunkserverConnectionException : public Exception {
public:
	ChunkserverConnectionException(const std::string& message, const NetworkAddress& server)
			: Exception(message + " (server " + server.toString() + ")"),
			  server_(server) {
	}

	~ChunkserverConnectionException() throw() {}
	const NetworkAddress& server() const throw() { return server_; }

private:
	NetworkAddress server_;
};

class ChunkCrcException : public RecoverableReadException {
public:
	ChunkCrcException(const std::string& message, const NetworkAddress& server,
			const ChunkType& chunkType)
			: RecoverableReadException(message + " (server " + server.toString() + ")"),
			  server_(server), chunkType_(chunkType) {
	}

	~ChunkCrcException() throw() {}
	const NetworkAddress& server() const throw() { return server_; }
	const ChunkType& chunkType() const throw() { return chunkType_; }

private:
	NetworkAddress server_;
	ChunkType chunkType_;
};

#endif // LIZARDFS_MOUNT_EXCEPTIONS_H_
