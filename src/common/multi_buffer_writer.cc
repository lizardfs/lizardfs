#include "common/platform.h"
#include "common/multi_buffer_writer.h"

#include <inttypes.h>

#ifdef _WIN32
#include "common/sockets.h"

ssize_t writev(int fd, const struct iovec *iov, int iovcnt) {
	ssize_t ret = 0;
	ssize_t send_ret = 0;

	for (int i = 0; i < iovcnt; ++i) {
		const struct iovec &buffer = iov[i];

		if (i > 0) { // check if next write would block
			int poll_ret = tcptopoll(fd, POLLOUT, 0);
			// If poll failed or next write would block, return partial write
			// if anything was written already or error if nothing was written yet
			if (poll_ret <= 0) {
				ret = ret ? ret : poll_ret;
				break;
			}
		}

		send_ret = tcpsend(fd, buffer.iov_base, buffer.iov_len);

		// If write failed, return partial write or error if nothing was written yet
		if (send_ret < 0) {
			ret = ret ? ret : send_ret;
			break;
		}

		ret += send_ret;

		// If partial write occurred, return
		if ((size_t)send_ret != buffer.iov_len) {
			break;
		}
	}
	return ret;
}

#endif

void MultiBufferWriter::addBufferToSend(const void* buffer, size_t size) {
	struct iovec iov;
	iov.iov_base = (void*)buffer; // remove 'const' as we will pass this iovec to writev
	iov.iov_len = size;
	buffers_.push_back(iov);
}

ssize_t MultiBufferWriter::writeTo(int fd) {
	ssize_t ret = writev(fd,
		buffers_.data() + buffersCompletelySent_,
		buffers_.size() - buffersCompletelySent_);
	if (ret < 0) {
		return ret;
	}
	size_t bytesToBeRemovedFromIovec =      ret;
	while (bytesToBeRemovedFromIovec > 0) {
		struct iovec& nextBuffer = buffers_[buffersCompletelySent_];
		if (nextBuffer.iov_len <= bytesToBeRemovedFromIovec) {
			bytesToBeRemovedFromIovec -= nextBuffer.iov_len;
			buffersCompletelySent_++;
		} else {
			nextBuffer.iov_base = (uint8_t*)nextBuffer.iov_base + bytesToBeRemovedFromIovec;
			nextBuffer.iov_len -= bytesToBeRemovedFromIovec;
			bytesToBeRemovedFromIovec = 0;
		}
	}
	return ret;
}
