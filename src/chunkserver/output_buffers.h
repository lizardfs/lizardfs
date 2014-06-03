#pragma once

#include "config.h"

#include <stdlib.h>
#include <cstdint>
#include <cstring>
#include <vector>

#define HAVE_SPLICE 1 // TODO(alek) zmienna do konfiga

class OutputBuffer {
public:
	enum WriteStatus {
		WRITE_DONE,
		WRITE_AGAIN,
		WRITE_ERROR
	};

	virtual ssize_t copyIntoBuffer(int inputFileDescriptor, size_t len, off_t* offset) = 0;
	virtual ssize_t copyIntoBuffer(const void *mem, size_t len) = 0;
	ssize_t copyIntoBuffer(const std::vector<uint8_t>& mem) {
		return copyIntoBuffer(mem.data(), mem.size());
	}
	virtual WriteStatus writeOutToAFileDescriptor(int outputFileDescriptor) = 0;
	virtual size_t bytesInABuffer() const = 0;
	virtual ~OutputBuffer() {};
};

#ifdef HAVE_SPLICE
class AvoidingCopyingOutputBuffer : public OutputBuffer {
public:
	AvoidingCopyingOutputBuffer(size_t internalBufferCapacity);
	~AvoidingCopyingOutputBuffer();

	void resetOutputFileDescriptor(int newOutputFileDescriptor);

	ssize_t copyIntoBuffer(int inputFileDescriptor, size_t len, off_t* offset);
	ssize_t copyIntoBuffer(const void *mem, size_t len);

	WriteStatus writeOutToAFileDescriptor(int outputFileDescriptor);

	size_t bytesInABuffer() const;

private:
	int internalPipeFileDescriptors_[2];
	const size_t internalBufferCapacity_;

	size_t bytesInABuffer_;
};
#endif /* HAVE_SPLICE */

class SimpleOutputBuffer : public OutputBuffer {
public:
	SimpleOutputBuffer(size_t internalBufferCapacity);
	~SimpleOutputBuffer();

	ssize_t copyIntoBuffer(int inputFileDescriptor, size_t len, off_t* offset);
	ssize_t copyIntoBuffer(const void *mem, size_t len);

	WriteStatus writeOutToAFileDescriptor(int outputFileDescriptor);

	size_t bytesInABuffer() const;
	const uint8_t* data() const {
		return buffer_.data();
	}

private:
	const size_t internalBufferCapacity_;
	std::vector<uint8_t> buffer_;
	size_t bufferUnflushedDataFirstIndex_;
	size_t bufferUnflushedDataOneAfterLastIndex_;
};
