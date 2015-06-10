#include "common/platform.h"
#include "chunkserver/output_buffers.h"

#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <ios>
#include <stdexcept>

#include "common/massert.h"
#include "devtools/request_log.h"

#ifdef LIZARDFS_HAVE_SPLICE
AvoidingCopyingOutputBuffer::AvoidingCopyingOutputBuffer(size_t internalBufferCapacity)
	: internalBufferCapacity_(internalBufferCapacity),
	  bytesInABuffer_(0) {
	LOG_AVG_TILL_END_OF_SCOPE0("pipe2");
	eassert(internalBufferCapacity_ > 0);
	eassert(pipe2(internalPipeFileDescriptors_, O_NONBLOCK) != -1);
#ifdef F_SETPIPE_SZ
	if (fcntl(internalPipeFileDescriptors_[1], F_SETPIPE_SZ, internalBufferCapacity_) == -1) {
		for (int i = 0; i <=1; ++i) {
			eassert(close(internalPipeFileDescriptors_[i]) >= 0);
		}
		massert(false,
				"fcntl(internalPipeFileDescriptors[1], F_SETPIPE_SZ, internalBufferCapacity) >= 0");
	}
#endif
}

AvoidingCopyingOutputBuffer::~AvoidingCopyingOutputBuffer() {
	LOG_AVG_TILL_END_OF_SCOPE0("pipe_close");
	eassert(close(internalPipeFileDescriptors_[0]) != -1);
	eassert(close(internalPipeFileDescriptors_[1]) != -1);
}

ssize_t AvoidingCopyingOutputBuffer::copyIntoBuffer(
		int inputFileDescriptor, size_t len, off_t* offset) {
	eassert(len + bytesInABuffer_ <= internalBufferCapacity_);
	off_t bytesWritten = 0;
	loff_t inputOffset;
	while (len > 0) {
		if (offset) {
			inputOffset = *offset + bytesWritten;
		}
		ssize_t ret = splice(inputFileDescriptor, (offset ? &inputOffset : NULL),
				internalPipeFileDescriptors_[1], NULL, len, SPLICE_F_MOVE);
		if (ret <= 0) {
			return bytesWritten;
		}
		len -= ret;
		bytesInABuffer_ += ret;
		bytesWritten += ret;
	}
	return bytesWritten;
}

ssize_t AvoidingCopyingOutputBuffer::copyIntoBuffer(const void *mem, size_t len) {
	LOG_AVG_TILL_END_OF_SCOPE0("splice_file");
	eassert(len + bytesInABuffer_ <= internalBufferCapacity_);
	ssize_t bytes_written = 0;
	while (len > 0) {
		ssize_t ret = ::write(internalPipeFileDescriptors_[1], (uint8_t*)mem + bytes_written, len);
		if (ret <= 0) {
			return bytes_written;
		}
		len -= ret;
		bytesInABuffer_ += ret;
		bytes_written += ret;
	}
	return bytes_written;
}

OutputBuffer::WriteStatus AvoidingCopyingOutputBuffer::writeOutToAFileDescriptor(
		int outputFileDescriptor) {
	LOG_AVG_TILL_END_OF_SCOPE0("splice_socket");
	while (bytesInABuffer_ > 0) {
		ssize_t ret = splice(internalPipeFileDescriptors_[0], NULL,
				outputFileDescriptor, NULL, bytesInABuffer_, SPLICE_F_MOVE);
		if (ret <= 0) {
			if (errno == EAGAIN) {
				return WRITE_AGAIN;
			}
			return WRITE_ERROR;
		}
		bytesInABuffer_ -= ret;
	}
	return WRITE_DONE;
}

size_t AvoidingCopyingOutputBuffer::bytesInABuffer() const {
	return bytesInABuffer_;
}
#endif /* LIZARDFS_HAVE_SPLICE */

SimpleOutputBuffer::SimpleOutputBuffer(size_t internalBufferCapacity)
	: internalBufferCapacity_(internalBufferCapacity),
	  buffer_(internalBufferCapacity, 0),
	  bufferUnflushedDataFirstIndex_(0),
	  bufferUnflushedDataOneAfterLastIndex_(0)
{
	eassert(internalBufferCapacity > 0);
	buffer_.reserve(internalBufferCapacity_);
}

OutputBuffer::WriteStatus SimpleOutputBuffer::writeOutToAFileDescriptor(int outputFileDescriptor) {
	while (bytesInABuffer() > 0) {
		ssize_t ret = ::write(outputFileDescriptor, &buffer_[bufferUnflushedDataFirstIndex_],
				bytesInABuffer());
		if (ret <= 0) {
			if (errno == EAGAIN) {
				return WRITE_AGAIN;
			}
			return WRITE_ERROR;
		}
		bufferUnflushedDataFirstIndex_ += ret;
	}
	return WRITE_DONE;
}

size_t SimpleOutputBuffer::bytesInABuffer() const {
	return bufferUnflushedDataOneAfterLastIndex_ - bufferUnflushedDataFirstIndex_;
}

void SimpleOutputBuffer::clear() {
	bufferUnflushedDataFirstIndex_ = 0;
	bufferUnflushedDataOneAfterLastIndex_ = 0;
}

ssize_t SimpleOutputBuffer::copyIntoBuffer(int inputFileDescriptor, size_t len, off_t* offset) {
	eassert(len + bufferUnflushedDataOneAfterLastIndex_ <= internalBufferCapacity_);
	off_t bytes_written = 0;
	while (len > 0) {
		ssize_t ret = pread(inputFileDescriptor, (void*)&buffer_[bufferUnflushedDataOneAfterLastIndex_], len,
				(offset ? *offset : 0));
		if (ret <= 0) {
			return bytes_written;
		}
		len -= ret;
		bufferUnflushedDataOneAfterLastIndex_ += ret;
		bytes_written += ret;
	}
	return bytes_written;
}

ssize_t SimpleOutputBuffer::copyIntoBuffer(const void *mem, size_t len) {
	eassert(bufferUnflushedDataOneAfterLastIndex_ + len <= internalBufferCapacity_);
	memcpy((void*)&buffer_[bufferUnflushedDataOneAfterLastIndex_], mem, len);
	bufferUnflushedDataOneAfterLastIndex_ += len;

	return len;
}

SimpleOutputBuffer::~SimpleOutputBuffer() {
}
