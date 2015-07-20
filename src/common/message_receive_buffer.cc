#include "common/platform.h"
#include "common/message_receive_buffer.h"
#include "common/sockets.h"

#include <unistd.h>
#include <cstring>

ssize_t MessageReceiveBuffer::readFrom(int fd) {
	sassert(bytesReveived_ < buffer_.size());
	int ret = tcprecv(fd, buffer_.data() + bytesReveived_, buffer_.size() - bytesReveived_);
	if (ret < 0) {
		return ret;
	}
	bytesReveived_ += ret;
	return ret;
}

void MessageReceiveBuffer::removeMessage() {
	sassert(hasMessageData());
	size_t totalMessageSize = PacketHeader::kSize + getMessageHeader().length;
	if (bytesReveived_ > totalMessageSize) {
		size_t extraDataSize = bytesReveived_ - totalMessageSize;
		memmove(buffer_.data(), buffer_.data() + totalMessageSize, extraDataSize);
	}
	bytesReveived_ -= totalMessageSize;
}
