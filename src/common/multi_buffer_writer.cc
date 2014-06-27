#include "common/platform.h"
#include "common/multi_buffer_writer.h"

#include <inttypes.h>

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
	size_t bytesToBeRemovedFromIovec = ret;
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
