/*
   Copyright 2017 Skytechnology sp. z o.o..

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/platform.h"

#include <cassert>
#include <algorithm>
#include <cstdint>
#include <cstring>

#ifndef _WIN32
	#include <sys/uio.h>
#else // if defined(_WIN32)
	#include <unistd.h>

	struct iovec {
		void *iov_base;
		size_t iov_len;
	};
#endif

/*!
 * \brief Copy contiguous memory to io-vector
 * \param iov io-vector to be filled
 * \param iovcnt number of entries in iov
 * \param buf memory to be copied
 * \param len buf's size
 */
inline void memcpyIoVec(const struct iovec *iov, int iovcnt, const char *buf, size_t len) {
	assert(iov != nullptr);
	assert(iovcnt >= 0);
	int iov_index = 0;
	while (len > 0 && iov_index < iovcnt) {
		if (iov->iov_base && iov->iov_len > 0) {
			size_t to_copy = std::min(iov->iov_len, len);
			std::memcpy(iov->iov_base, buf, to_copy);
			buf += to_copy;
			len -= to_copy;
		}
		iov_index++;
		iov++;
	}
}

/*!
 * \brief Copy io-vector to another io-vector.
 * \param buf io-vector to be filled
 * \param bufcnt number of entries in buf
 * \param iov io-vector to be copied
 * \param iovcnt number of entries in iov
 * \return number of bytes copied
 */
inline size_t copyIoVec(const struct iovec *buf, int bufcnt, const struct iovec *iov, int iovcnt) {
	assert(buf != nullptr);
	assert(iov != nullptr);
	assert(bufcnt >= 0);
	assert(iovcnt >= 0);
	char *iov_base = (char *)iov->iov_base;
	char *buf_base = (char *)buf->iov_base;
	size_t iov_len = iov->iov_len;
	size_t buf_len = buf->iov_len;
	int iov_index = 0;
	int buf_index = 0;
	size_t total = 0;
	while (buf_index < bufcnt && iov_index < iovcnt) {
		size_t len = std::min(iov_len, buf_len);
		total += len;
		std::memcpy(buf_base, iov_base, len);

		iov_base += len;
		iov_len -= len;
		while (iov_index < iovcnt && iov_len == 0) {
			iov_index++;
			iov++;
			iov_base = (char *)iov->iov_base;
			iov_len  = iov->iov_len;
		}

		buf_base += len;
		buf_len -= len;
		while (buf_index < bufcnt && buf_len == 0) {
			buf_index++;
			buf++;
			buf_base = (char *)buf->iov_base;
			buf_len  = buf->iov_len;
		}
	}

	return total;
}
