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

#pragma once

#include "common/platform.h"

#include <stdlib.h>
#include <cstdint>
#include <cstring>
#include <vector>

class OutputBuffer {
public:
	enum WriteStatus {
		WRITE_DONE,
		WRITE_AGAIN,
		WRITE_ERROR
	};

	OutputBuffer(size_t internalBufferCapacity);
	~OutputBuffer();

	ssize_t copyIntoBuffer(int inputFileDescriptor, size_t len, off_t* offset);
	ssize_t copyIntoBuffer(const void *mem, size_t len);

	ssize_t copyIntoBuffer(const std::vector<uint8_t>& mem) {
		return copyIntoBuffer(mem.data(), mem.size());
	}

	WriteStatus writeOutToAFileDescriptor(int outputFileDescriptor);

	size_t bytesInABuffer() const;
	const uint8_t* data() const {
		return buffer_.data();
	}
	void clear();

private:
	const size_t internalBufferCapacity_;
	std::vector<uint8_t> buffer_;
	size_t bufferUnflushedDataFirstIndex_;
	size_t bufferUnflushedDataOneAfterLastIndex_;
};
