#ifndef LIZARDFS_COMMON_MULTI_BUFFER_WRITER_H
#define LIZARDFS_COMMON_MULTI_BUFFER_WRITER_H

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

#endif /* LIZARDFS_COMMON_MULTI_BUFFER_WRITER_H */
