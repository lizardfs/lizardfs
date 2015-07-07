/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

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

#include <sys/uio.h>
#include <vector>

/*
 * A class which helps sending concatenated buffers (eg. a message header and it's data)
 *
 * TODO(msulikowski) If we need to support systems without writev we have to do something
 * with this implementation. But I don't know if there are any C++11 capable systems without writev.
 */
class MultiBufferWriter {
public:
	MultiBufferWriter() {
		reset();
	}

	void reset() {
		buffers_.clear();
		buffersCompletelySent_ = 0;
	}

	bool hasDataToSend() const {
		return buffersCompletelySent_ < buffers_.size();
	}

	void addBufferToSend(const void* buffer, size_t size);
	ssize_t writeTo(int fd);

private:
	std::vector<struct iovec> buffers_;
	size_t buffersCompletelySent_;
};
