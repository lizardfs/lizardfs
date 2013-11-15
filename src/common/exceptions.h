#ifndef LIZARDFS_MFSMOUNT_EXCEPTIONS_H_
#define LIZARDFS_MFSMOUNT_EXCEPTIONS_H_

#include "common/chunk_type.h"
#include "common/mfsstrerr.h"
#include "common/network_address.h"

class Exception : public std::exception {
public:
	Exception(const std::string& message) : message_(message), status_(STATUS_OK) {
	}

	Exception(const std::string& message, uint8_t status) : message_(message), status_(status) {
		if (status != STATUS_OK) {
			message_ += "(" + std::string(mfsstrerr(status)) + ")";
		}
	}

	~Exception() throw() {}

	const char* what() const throw() {
		return message_.c_str();
	}

	uint8_t status() const throw() {
		return status_;
	}

private:
	std::string message_;
	uint8_t status_;
};

#define LIZARDFS_CREATE_EXCEPTION_CLASS(name, base) \
	class name : public base { \
	public: \
		name(const std::string& message) : base(message) {} \
		name(const std::string& message, uint8_t status) : base(message, status) {} \
		~name() throw() {} \
	}

LIZARDFS_CREATE_EXCEPTION_CLASS(ReadException, Exception);
LIZARDFS_CREATE_EXCEPTION_CLASS(RecoverableReadException, ReadException);
LIZARDFS_CREATE_EXCEPTION_CLASS(UnrecoverableReadException, ReadException);
LIZARDFS_CREATE_EXCEPTION_CLASS(NoValidCopiesReadException, RecoverableReadException);

class ChunkserverConnectionException : public RecoverableReadException {
public:
	ChunkserverConnectionException(const std::string& message, const NetworkAddress& server)
			: RecoverableReadException(message + " (server " + server.toString() + ")"),
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

#endif // LIZARDFS_MFSMOUNT_EXCEPTIONS_H_
