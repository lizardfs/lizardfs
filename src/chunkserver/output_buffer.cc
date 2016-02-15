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
#include <fcntl.h>
#include <unistd.h>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <ios>
#include <stdexcept>

#include "chunkserver/output_buffer.h"
#include "common/crc.h"
#include "common/massert.h"
#include "devtools/request_log.h"

OutputBuffer::OutputBuffer(size_t internalBufferCapacity)
	: internalBufferCapacity_(internalBufferCapacity),
	  buffer_(internalBufferCapacity, 0),
	  bufferUnflushedDataFirstIndex_(0),
	  bufferUnflushedDataOneAfterLastIndex_(0)
{
	eassert(internalBufferCapacity > 0);
	buffer_.reserve(internalBufferCapacity_);
}

OutputBuffer::WriteStatus OutputBuffer::writeOutToAFileDescriptor(int outputFileDescriptor) {
	while (bytesInABuffer() > 0) {
		ssize_t ret = ::write(outputFileDescriptor, &buffer_[bufferUnflushedDataFirstIndex_],
				bytesInABuffer());
		if (ret <= 0) {
			if (ret == 0 || errno == EAGAIN) {
				return WRITE_AGAIN;
			}
			return WRITE_ERROR;
		}
		bufferUnflushedDataFirstIndex_ += ret;
	}
	return WRITE_DONE;
}

size_t OutputBuffer::bytesInABuffer() const {
	return bufferUnflushedDataOneAfterLastIndex_ - bufferUnflushedDataFirstIndex_;
}

void OutputBuffer::clear() {
	bufferUnflushedDataFirstIndex_ = 0;
	bufferUnflushedDataOneAfterLastIndex_ = 0;
}

ssize_t OutputBuffer::copyIntoBuffer(int inputFileDescriptor, size_t len, off_t* offset) {
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

bool OutputBuffer::checkCRC(size_t bytes, uint32_t crc) const {
	assert(bufferUnflushedDataOneAfterLastIndex_ - bytes > 0
			&& bufferUnflushedDataOneAfterLastIndex_ - bytes < buffer_.size());
	return mycrc32(0, &buffer_[bufferUnflushedDataOneAfterLastIndex_ - bytes], bytes) == crc;
}

ssize_t OutputBuffer::copyIntoBuffer(const void *mem, size_t len) {
	eassert(bufferUnflushedDataOneAfterLastIndex_ + len <= internalBufferCapacity_);
	memcpy((void*)&buffer_[bufferUnflushedDataOneAfterLastIndex_], mem, len);
	bufferUnflushedDataOneAfterLastIndex_ += len;

	return len;
}

OutputBuffer::~OutputBuffer() {
}
