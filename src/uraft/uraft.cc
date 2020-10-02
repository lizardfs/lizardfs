#include "common/platform.h"
#include "uraft.h"

#if defined(LIZARDFS_HAVE_GETIFADDRS)
 #include <sys/types.h>
 #include <ifaddrs.h>
#endif

#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

using boost::asio::ip::udp;

uRaft::uRaft(boost::asio::io_service &ios)
	: io_service_(ios),
	  socket_(ios),
	  election_timer_(ios),
	  heartbeat_timer_(ios),
	  loyalty_agreement_timer_(ios) {
	opt_.election_timeout_min   = 300;
	opt_.election_timeout_max   = 500;
	opt_.heartbeat_period       = 20;
	opt_.id                     = -1;
	opt_.port                   = 9425;

	state_.id           = 0;
	state_.type         = kFollower;
	state_.current_term = 0;
	state_.data_version = 0;
	state_.leader_id    = -1;
	state_.voted_for    = -1;
	state_.president    = false;
	state_.local_time   = 0;

	block_leader_promotion_ = false;
}

uRaft::~uRaft() {
}

void uRaft::nodePromote() {
}

void uRaft::nodeDemote() {
}

void uRaft::nodeLeader(int) {
}

uint64_t uRaft::nodeGetVersion() {
	return state_.data_version;
}

template<typename ConstBufferSequence>
void uRaft::socketSend(const ConstBufferSequence &buffers, const boost::asio::ip::udp::endpoint &destination) {
	boost::system::error_code ecode;
	socket_.send_to(buffers, destination, 0, ecode);
}

/*! \brief Returns node id.
 *
 * \param addr IP address to match
 * \return index of node with address addr
 *         -1 when there is no node matching address addr
 */
int uRaft::findNodeID(const boost::asio::ip::udp::endpoint &addr) {
	for (int i = 0; i < (int)node_.size(); i++) {
		if (node_[i].addr == addr) return i;
	}
	return -1;
}

/*! \brief Return number of nodes voting for this node.
 *
 * \param count_loyal Count only loyal nodes.
 */
int uRaft::voteCount(bool count_loyal) {
	int votes = 0;
	for (const auto &node : node_) {
		int time_from_loyalty_vote = opt_.heartbeat_period *
		                             ((state_.local_time - node.heartbeat) + 1);

		votes += (!count_loyal && node.vote_granted) ||
		         (count_loyal && time_from_loyalty_vote <= opt_.election_timeout_min);
	}
	return votes;
}

//! Check if received packet has newer term than ours. If so we switch to follower state.
void uRaft::checkTerm(int /*id*/, const RpcHeader &data) {
	if (data.term > state_.current_term) {
		if (state_.president) {
			assert(state_.type != kFollower);
			state_.current_term = data.term;
			electionTimeout(boost::system::error_code());
			return;
		}

		if (state_.type != kFollower) startElectionTimer();
		state_.type         = kFollower;
		state_.voted_for    = -1;
		state_.current_term = data.term;
	}
}

/*! \brief Checks if packet is valid.
 *
 * Packet with lower term than current is discarded by Raft algorithm. So we can consider it as
 * invalid.
 */
bool uRaft::validPacket(const uint8_t *data, size_t size) {
	static unsigned packet_size[kRpcLast] = {sizeof(RpcRequest), sizeof(RpcRequest),
	                                        sizeof(RpcResponse), sizeof(RpcResponse)
	                                        };

	if (data[0] >= kRpcLast || size != packet_size[data[0]]) {
		return false;
	}

	return reinterpret_cast<const RpcHeader *>(data)->term >= state_.current_term;
}

void uRaft::startElectionTimer() {
	int timeout = opt_.election_timeout_min +
	              rand() % (opt_.election_timeout_max - opt_.election_timeout_min);
	election_timer_.expires_from_now(boost::posix_time::millisec(timeout));
	election_timer_.async_wait(boost::bind(&uRaft::electionTimeout, this,
	                                       boost::asio::placeholders::error));
}

void uRaft::startHearbeatTimer() {
	heartbeat_timer_.expires_from_now(boost::posix_time::millisec(opt_.heartbeat_period));
	heartbeat_timer_.async_wait(boost::bind(&uRaft::heartbeat, this,
	                                        boost::asio::placeholders::error));
}

void uRaft::signLoyaltyAgreement() {
	state_.loyalty_agreement = true;
	loyalty_agreement_timer_.expires_from_now(boost::posix_time::millisec(opt_.election_timeout_min));
	loyalty_agreement_timer_.async_wait([this](const boost::system::error_code & error) {
		if (!error) {
			state_.loyalty_agreement = false;
		}
	});
}

void uRaft::startReceive() {
	socket_.async_receive_from(boost::asio::buffer(packet_data_, kMaxPacketLength), sender_endpoint_,
	                           boost::bind(&uRaft::receivePacket, this,
	                                       boost::asio::placeholders::error,
	                                       boost::asio::placeholders::bytes_transferred));
}

void uRaft::sendHeartbeat() {
	RpcRequest req;

	req.type          = kRpcAppendEntries;
	req.term          = state_.current_term;
	req.time          = state_.local_time;
	req.data_version  = state_.data_version;
	req.node_id       = state_.id;

	for (int i = 0; i < (int)node_.size(); i++) {
		if ((int)i == state_.id) continue;
		socketSend(boost::asio::buffer(&req, sizeof(req)), node_[i].addr);
	}
}

void uRaft::sendRequestForVotes() {
	RpcRequest req;

	req.type         = kRpcRequestVote;
	req.term         = state_.current_term;
	req.time         = state_.local_time;
	req.data_version = state_.data_version;
	req.node_id      = state_.id;

	for (const auto &node : node_) {
		if (node.recv) continue;
		socketSend(boost::asio::buffer(&req, sizeof(req)), node.addr);
	}
}

void uRaft::electionTimeout(const boost::system::error_code &error) {
	if (error) {
		return;
	}

	if (block_leader_promotion_) {
		startElectionTimer();
		return;
	}

	state_.type = kCandidate;
	state_.current_term++;
	state_.voted_for = state_.id;

	for (auto &node : node_) {
		node.vote_granted = false;
		node.recv = false;
	}

	node_[state_.id].vote_granted    = true;
	node_[state_.id].recv            = true;
	node_[state_.id].data_version    = state_.data_version;
	node_[state_.id].heartbeat       = state_.local_time;

	if (opt_.quorum > 1) {
		state_.data_version = nodeGetVersion();
		sendRequestForVotes();
		startElectionTimer();
	} else {
		state_.type = kLeader;
		nodePromote();
	}
}

/*! \brief Called every heartbeat_period ms. */
void uRaft::heartbeat(const boost::system::error_code &error) {
	if (error) {
		return;
	}

	startHearbeatTimer();

	state_.local_time++;
	node_[state_.id].heartbeat = state_.local_time;

	// Roll back from being the leader if there are less than quorum
	// loyal nodes alive
	if (state_.president) {
		assert(state_.type != kFollower);
		if (voteCount(true) < opt_.quorum) {
			if (state_.type == kLeader) {
				state_.type      = kFollower;
				state_.voted_for = -1;
				startElectionTimer();
			}
			state_.president = false;

			nodeDemote();

			return;
		}
	}

	if (state_.type == kCandidate) {
		sendRequestForVotes();
	}

	if (state_.type == kLeader) {
		sendHeartbeat();
		if (!state_.president) {
			if (voteCount(true) >= opt_.quorum) {
				state_.president = true;
				nodePromote();
			}
		}
	}
}

/*! \brief Handling of RPC Append Entries packet. */
void uRaft::rpcAppend(int id, const RpcRequest &data) {
	RpcResponse res;

	assert(state_.type != kLeader);
	assert(data.term >= state_.current_term);

	if (id != state_.leader_id) {
		state_.data_version = nodeGetVersion();
	}

	res.type         = kRpcAEResponse;
	res.term         = state_.current_term;
	res.result       = 1;
	res.req_time     = data.time;
	res.data_version = state_.data_version;

	if (id != state_.leader_id) nodeLeader(id);
	state_.leader_id = id;
	signLoyaltyAgreement();

	socketSend(boost::asio::buffer(&res, sizeof(res)), sender_endpoint_);

	state_.type = kFollower;
	startElectionTimer();
}

/*! \brief Handling of RPC Append Response packet. */
void uRaft::rpcAppendResponse(int id, const RpcResponse &data) {
	if (state_.type != kLeader) {
		return;
	}

	node_[id].heartbeat    = std::max(node_[id].heartbeat,data.req_time);
	node_[id].data_version = data.data_version;
	node_[id].vote_granted = true;
	node_[id].recv         = true;
}

/*! \brief Handling of RPC Request Vote packet. */
void uRaft::rpcReqVote(int id, const RpcRequest &data) {
	RpcResponse res;

	if (state_.voted_for < 0) {
		state_.data_version = nodeGetVersion();
	}

	res.type         = kRpcRVResponse;
	res.term         = state_.current_term;
	res.req_time     = data.time;
	res.data_version = state_.data_version;
	res.result       = state_.type == kFollower &&
	                   data.term >= state_.current_term &&
	                   data.data_version >= state_.data_version &&
	                   ((state_.loyalty_agreement && state_.leader_id == id) ||
	                    (!state_.loyalty_agreement &&
	                     (state_.voted_for < 0 || state_.voted_for == id)));

	socketSend(boost::asio::buffer(&res, sizeof(res)), sender_endpoint_);

	if (res.result) {
		state_.voted_for = id;
		startElectionTimer();
	}
}

/*! \brief Handling of RPC Request Vote Response packet. */
void uRaft::rpcReqVoteResponse(int id, const RpcResponse &data) {
	if (state_.type != kCandidate) {
		return;
	}

	node_[id].vote_granted = data.result;
	node_[id].recv         = true;

	if (voteCount(false) >= opt_.quorum) {
		state_.type      = kLeader;
		state_.leader_id = state_.id;
		sendHeartbeat();
		election_timer_.cancel();
	}
}

/*! \brief Dispatching received packet to proper handler. */
void uRaft::receivePacket(const boost::system::error_code &error,  size_t bytes_recvd) {
	if (!error && bytes_recvd > 0) {
		int id = findNodeID(sender_endpoint_);

		if (id >= 0 && validPacket(packet_data_.data(), bytes_recvd)) {
			checkTerm(id, *reinterpret_cast<RpcHeader *>(packet_data_.data()));
			switch (packet_data_[0]) {
			case kRpcAppendEntries :
				rpcAppend(id, *reinterpret_cast<RpcRequest *>(packet_data_.data()));
				break;
			case kRpcRequestVote:
				rpcReqVote(id, *reinterpret_cast<RpcRequest *>(packet_data_.data()));
				break;
			case kRpcAEResponse:
				rpcAppendResponse(id, *reinterpret_cast<RpcResponse *>(packet_data_.data()));
				break;
			case kRpcRVResponse:
				rpcReqVoteResponse(id, *reinterpret_cast<RpcResponse *>(packet_data_.data()));
				break;
			}
		}
	}

	startReceive();
}

void uRaft::init() {
	udp::resolver resolver(io_service_);

	node_.resize(opt_.server.size());

	for (auto & node : node_) {
		node.data_version = 0;
		node.heartbeat    = 0;
		node.recv         = false;
		node.vote_granted = false;
	}

	for (int i = 0; i < (int)node_.size(); i++) {
		std::string::size_type p = opt_.server[i].find(":");
		boost::system::error_code err;
		udp::resolver::iterator   iendpoint;

		if (p != std::string::npos) {
			udp::resolver::query query(udp::v4(), opt_.server[i].substr(0, p),
			                           opt_.server[i].substr(p + 1));

			iendpoint = resolver.resolve(query, err);
			if (err) {
				throw std::runtime_error("Failed to resolve dns name '" + opt_.server[i] + "'");
			}
		} else {
			udp::resolver::query query(udp::v4(), opt_.server[i],
			                           boost::lexical_cast<std::string>(opt_.port));
			iendpoint = resolver.resolve(query, err);
			if (err) {
				throw std::runtime_error("Failed to resolve dns name '" + opt_.server[i]
				                         + ":" + boost::lexical_cast<std::string>(opt_.port)+"'");
			}
		}

		node_[i].addr = *iendpoint;
	}

	state_.id = opt_.id;
	if (state_.id < 0) {
		state_.id = scanLocalInterfaces();
	}
	if (state_.id < 0) {
		throw std::runtime_error("Invalid id");
	}

	opt_.quorum = opt_.server.size() / 2 + 1;
	state_.local_time   = opt_.election_timeout_min / opt_.heartbeat_period + 1;

	socket_.open(boost::asio::ip::udp::v4());
	socket_.bind(node_[state_.id].addr);

	startElectionTimer();
	startHearbeatTimer();
	startReceive();

	// In case of fast restart
	// It's possible that before restart we have signed agreement with a leader.
	// Now we wait for this agreement to expire.
	signLoyaltyAgreement();
}

void uRaft::demoteLeader() {
	if (state_.type == kFollower) {
		return;
	}
	state_.type      = kFollower;
	state_.voted_for = -1;
	state_.leader_id = -1;
	state_.president = false;

	startElectionTimer();
}

void uRaft::set_block_promotion(bool block) {
	block_leader_promotion_ = block;
}

void uRaft::set_options(const Options &opt) {
	opt_ = opt;
}

int uRaft::findMatchingAddress(const boost::asio::ip::address &addr, int &id) {
	int count = 0;

	for (int i = 0; i < (int)node_.size(); i++) {
		if (node_[i].addr.address() == addr) {
			count++;
			id = i;
		}
	}

	return count;
}

int uRaft::scanLocalInterfaces() {
#if defined(LIZARDFS_HAVE_GETIFADDRS)
	struct ifaddrs *ifaddr, *ifa;

	if (getifaddrs(&ifaddr) == -1) {
		return -1;
	}

	int count = 0, id = -1;
	for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
		if (ifa->ifa_addr == NULL) {
			continue;
		}

		int family = ifa->ifa_addr->sa_family;
		if (family != AF_INET) {
			continue;
		}

		sockaddr_in *saddr = reinterpret_cast<sockaddr_in *>(ifa->ifa_addr);
		boost::asio::ip::address addr = boost::asio::ip::address_v4(saddr->sin_addr.s_addr);

		count += findMatchingAddress(addr, id);
	}

	freeifaddrs(ifaddr);

	if (count != 1) {
		return -1;
	}

	assert(id >= 0);
	return id;
#else
	return -1;
#endif
}
