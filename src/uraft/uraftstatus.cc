#include "common/platform.h"
#include "uraftstatus.h"

#include <iostream>
#include <boost/format.hpp>

uRaftStatusConnection::uRaftStatusConnection(boost::asio::io_service &ios)
	: data_(),
	  socket_(ios) {
}

void uRaftStatusConnection::init() {
	auto self(shared_from_this());

	boost::asio::async_write(socket_, boost::asio::buffer(data_),
	[this, self](const boost::system::error_code & /*error*/, std::size_t /*length*/) {
		//if (error) log();
	});
}

boost::asio::ip::tcp::socket &uRaftStatusConnection::socket() {
	return socket_;
}

uRaftStatus::uRaftStatus(boost::asio::io_service &ios)
	: uRaft(ios),
	  acceptor_(ios),
	  socket_(ios) {
}

uRaftStatus::~uRaftStatus() {
}

void uRaftStatus::init() {
	uRaft::init();

	boost::asio::ip::tcp::endpoint endpoint(boost::asio::ip::tcp::v4(), opt_.status_port);

	acceptor_.open(endpoint.protocol());
	acceptor_.bind(endpoint);
	acceptor_.listen(10);

	acceptConnection();
}

void uRaftStatus::set_options(const Options &opt) {
	uRaft::set_options(opt);
	opt_ = opt;
}

/*
void uRaftStatus::storeData(std::vector<uint8_t> &response) {
	std::vector<uRaftStatusNodeEntry> ndata;

	ndata.reserve(node_.size());
	for (const auto & node : node_) {
		uRaftStatusNodeEntry entry;

		entry.vote      = node.vote_granted;
		entry.heartbeat = node.heartbeat;
		entry.version   = node.data_version;
		entry.valid     = node.recv;

		ndata.push_back(entry);
	}

	matocl::uraftStatus::serialize(response,
	                               (uint32_t)opt_.id,
	                               (uint8_t)state_.type,
	                               (uint64_t)state_.current_term,
	                               (int32_t)state_.voted_for,
	                               (int32_t)state_.leader_id,
	                               (uint64_t)state_.data_version,
	                               ndata);
}
*/

void uRaftStatus::storeData(std::vector<uint8_t> &response) {
	static const char *state_txt[kStateLast] = {"FOLLOWER", "CANDIDATE", "LEADER"};
	std::string res;

	res  = str(boost::format("SERVER ID %i\n") % opt_.id);
	if (state_.president) res += "I'M THE BOOSSSS\n";
	res += str(boost::format("president=%i\n") % state_.president);
	res += str(boost::format("state=%s\n") % state_txt[state_.type]);
	res += str(boost::format("term=%i\n") % state_.current_term);
	res += str(boost::format("voted_for=%i\n") % state_.voted_for);
	res += str(boost::format("leader_id=%i\n") % state_.leader_id);
	res += str(boost::format("data_version=%i\n") % state_.data_version);
	res += str(boost::format("loyalty_agreement=%i\n") % state_.loyalty_agreement);
	res += str(boost::format("local_time=%i\n") % state_.local_time);
	res += str(boost::format("blocked_promote=%i\n") % block_leader_promotion_);

	if (state_.type != kFollower) {
		res += "votes=[";
		for (int i = 0; i < (int)node_.size(); i++) {
			if (i > 0) {
				res += "|";
			}
			res += str(boost::format("%8i") % node_[i].vote_granted);
		}
		res += "]\n";

		res += "heart=[";
		for (int i = 0; i < (int)node_.size(); i++) {
			if (i > 0) {
				res += "|";
			}
			int tm = opt_.heartbeat_period * (state_.local_time - node_[i].heartbeat);
			res += str(boost::format("%8.2f") % (tm / 1000.));
		}
		res += "]\n";

		res += "recv =[";
		for (int i = 0; i < (int)node_.size(); i++) {
			if (i > 0) {
				res += "|";
			}
			res += str(boost::format("%8i") % node_[i].recv);
		}
		res += "]\n";

		res += "ver  =[";
		for (int i = 0; i < (int)node_.size(); i++) {
			if (i > 0) {
				res += "|";
			}
			res += str(boost::format("%8i") % node_[i].data_version);
		}
		res += "]\n";
	}

	response.resize(res.size());
	std::copy(begin(res), end(res), begin(response));
}

void uRaftStatus::acceptConnection() {
	auto conn = std::make_shared<uRaftStatusConnection>(uRaft::io_service_);

	acceptor_.async_accept(conn->socket(),
	[this, conn](const boost::system::error_code & error) {
		if (!error) {
			storeData(conn->data_);
			conn->init();
		}

		acceptConnection();
	});
}
