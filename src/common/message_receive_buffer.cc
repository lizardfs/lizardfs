#include "common/message_receive_buffer.h"

#include <unistd.h>
#include <cstring>

ssize_t MessageReceiveBuffer::readFrom(int fd) {
	eassert(bytesReveived_ < buffer_.size())
	int ret = read(fd, buffer_.data() + bytesReveived_, buffer_.size() - bytesReveived_);
	if (ret < 0) {
		return ret;
	}
	bytesReveived_ += ret;
	return ret;
}

void MessageReceiveBuffer::removeMessage() {
	eassert(hasMessageData());
	size_t totalMessageSize = PacketHeader::kSize + getMessageHeader().length;
	if (bytesReveived_ > totalMessageSize) {
		size_t extraDataSize = bytesReveived_ - totalMessageSize;
		memmove(buffer_.data(), buffer_.data() + totalMessageSize, extraDataSize);
	}
	bytesReveived_ -= totalMessageSize;
}
