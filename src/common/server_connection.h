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

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>

#include "common/network_address.h"
#include "common/packet.h"

class ServerConnection {
public:
	enum class ReceiveMode {
		kReceiveFirstNonNopMessage, ///< Ignores ANTOAN_NOP responses
		kReceiveFirstMessage,       ///< Returns the first received response, even ANTOAN_NOP
	};

	ServerConnection(const std::string& host, const std::string& port);
	ServerConnection(const NetworkAddress& server);
	virtual ~ServerConnection();

	/// Sends a request and receives a response.
	/// Throws if there are connection errors or the received response is not
	/// of type \p expectedResponseType. \p request may be empty.
	/// \returns Body of the response (a message without a header)
	virtual MessageBuffer sendAndReceive(
			const MessageBuffer& request,
			PacketHeader::Type expectedResponseType,
			ReceiveMode receiveMode = ReceiveMode::kReceiveFirstNonNopMessage);

	/// A static method for those, who own a socket.
	static MessageBuffer sendAndReceive(
			int fd,
			const MessageBuffer& request,
			PacketHeader::Type expectedResponseType,
			ReceiveMode receiveMode = ReceiveMode::kReceiveFirstNonNopMessage);

protected:
	/// A socket (or -1 if closed)
	int fd_;

private:
	/// Opens a connection and sets \p fd_
	void connect(const NetworkAddress& server);
};

/// A special version of \p ServerConnection which sends \p ANTOAN_NOP every second.
/// Uses a background thread for this additional task
class KeptAliveServerConnection : public ServerConnection {
public:
	/// Inherited constructor
	KeptAliveServerConnection(const std::string& host, const std::string& port)
			: ServerConnection(host, port),
			  threadCanRun_(true) {
		startNopThread();
	}

	/// Inherited constructor
	KeptAliveServerConnection(const NetworkAddress& server)
			: ServerConnection(server),
			  threadCanRun_(true) {
		startNopThread();
	}

	/// A destructor which joins the the background thread
	virtual ~KeptAliveServerConnection();

	/// Overridden to synchronize with the background thread
	virtual MessageBuffer sendAndReceive(
			const MessageBuffer& request,
			PacketHeader::Type expectedResponseType,
			ReceiveMode receiveMode = ReceiveMode::kReceiveFirstNonNopMessage) override;

private:
	/// Initializes the \p nopThread_;
	void startNopThread();

	/// Set to false to stop the thread
	std::atomic<bool> threadCanRun_;

	/// For synchronization when writing fd_ or using cond
	std::mutex mutex_;

	/// For waiting between NOPs
	std::condition_variable cond_;

	/// A thread which sends ANTOAN_NOP every second
	std::thread nopThread_;
};
