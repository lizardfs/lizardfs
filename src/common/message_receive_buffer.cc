#include "common/platform.h"
#include "common/message_receive_buffer.h"

#include <unistd.h>
#include <cstring>

ssize_t MessageReceiveBuffer::readFrom(int fd) {
	eassert(bytesReceived_ < buffer_.size());
	int ret = read(fd, buffer_.data() + bytesReceived_, buffer_.size() - bytesReceived_);
	if (ret < 0) {
		return ret;
	}
	bytesReceived_ += ret;
	return ret;
}

void MessageReceiveBuffer::removeMessage() {
	eassert(hasMessageData());
	size_t totalMessageSize = PacketHeader::kSize + getMessageHeader().length;
	if (bytesReceived_ > totalMessageSize) {
		size_t extraDataSize = bytesReceived_ - totalMessageSize;
		memmove(buffer_.data(), buffer_.data() + totalMessageSize, extraDataSize);
	}
	bytesReceived_ -= totalMessageSize;
}
