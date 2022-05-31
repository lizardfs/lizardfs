#pragma once

#include "common/platform.h"

#include <boost/asio.hpp>
#include <boost/array.hpp>

/*! \brief Implementation of modified Raft consensus algorithm.
 *
 * Implements reduced version of Raft consensus algorithm (https://raftconsensus.github.io/)
 * Only the part responsible for selecting leader is used. Propagation of data and configuration
 * is removed. Additionally each node can use version variable for selecting node with highest value.
 * uRaft (micro - Raft) algorithm makes two guarantees:
 *  - there can be only one President at the time
 *  - selected President has greater or equal value of version variable than
 *    all other nodes making quorum.
 */
class uRaft {
public:
	struct Options {
		int                      id;
		int                      port;
		std::vector<std::string> server;
		int                      election_timeout_min;
		int                      election_timeout_max;
		int                      heartbeat_period;
		int                      quorum;
	};

protected:
	enum State {
	    kFollower=0,
	    kCandidate,
	    kLeader,
	    kStateLast
	};

	enum RpcType {
	    kRpcAppendEntries=0, /*!< RPC Append Entries. */
	    kRpcRequestVote,     /*!< RPC Request Vote. */
	    kRpcAEResponse,      /*!< RPC Append Entries Response. */
	    kRpcRVResponse,      /*!< RPC Request Vote Response. */
	    kRpcLast
	};

	static const int kMaxPacketLength = 1024;

	//! Information about node.
	struct NodeInfo {
		boost::asio::ip::udp::endpoint addr;         /*!< Node address. */
		uint64_t                       data_version; /*!< Node data version. */
		uint64_t                       heartbeat;    /*!< Local time of latest heartbeat response */
		bool                           vote_granted; /*!< True if node granted vote. */
		bool                           recv;         /*!< True if vote from node was received. */
	};

	//! Current node uRaft state.
	struct RaftState {
		int      id;                 /*!< Node id. */
		int      type;               /*!< Node state (follower,candidate,leader). */

		uint64_t current_term;       /*!< Current term number. */
		int32_t  voted_for;          /*!< Id of node that we voted for (can be -1 if node didn't vote). */
		int32_t  leader_id;          /*!< Id of the leader. */

		uint64_t local_time;         /*!< Local time measured in heartbeats. */
		bool     president;          /*!< True if node is a president. */
		bool     loyalty_agreement;  /*!< True if node is loyal to a leader. */

		uint64_t data_version;  /*!< This node data version (updated when required by calling nodeGetVersion). */
	};

	struct RpcHeader {
		uint8_t     type; /*!< RPC packet type */
		uint64_t    term;
	};

	struct RpcRequest : RpcHeader {
		int32_t  node_id;
		uint64_t time;
		uint64_t data_version;
	};

	struct RpcResponse : RpcHeader {
		uint8_t  result;
		uint64_t req_time;
		uint64_t data_version;
	};

public:
	uRaft(boost::asio::io_service &ios);
	virtual ~uRaft();

	//! Initialization of uRaft internal data.
	void init();

	//! Force node status change to follower.
	void demoteLeader();

	//! If true blocks change of status from follower to candidate or leader.
	void set_block_promotion(bool block);

	//! Set uRaft options.
	void set_options(const Options &opt);

	/*! \brief uRaft calls this on node becoming President.
	 *
	 * Function can be overridden in derived class to get notification on status change.
	 * Function should return as quickly as possible (otherwise can block uRaft working).
	 */
	virtual void     nodePromote();

	/*! \brief uRaft calls this function after node stop being President.
	 *
	 * Function can be overridden in derived class to get notification on status change.
	 * Function should return as quickly as possible (otherwise can block uRaft working).
	 */
	virtual void     nodeDemote();

	/*! \brief Called when uRaft gets information about new leader (which might soon become President).
	 *
	 * Function can be overridden in derived class to get notification about new leader id.
	 * Function should return as quickly as possible (otherwise can block uRaft working).
	 */
	virtual void     nodeLeader(int id);

	/*! \brief uRaft calls this function when it needs current data version.
	 *
	 * Function needs to be overridden in derived class and return current value of data version.
	 * Function should return as quickly as possible (otherwise can block uRaft working).
	 */
	virtual uint64_t nodeGetVersion();

protected:
	void checkTerm(int id, const RpcHeader &data);
	bool validPacket(const uint8_t *data, size_t size);
	int  findNodeID(const boost::asio::ip::udp::endpoint &addr);
	int  voteCount(bool count_loyal);

	void startElectionTimer();
	void startHearbeatTimer();
	void startReceive();
	void signLoyaltyAgreement();

	void sendHeartbeat();
	void sendRequestForVotes();

	void electionTimeout(const boost::system::error_code &error);
	void heartbeat(const boost::system::error_code &error);
	void receivePacket(const boost::system::error_code &error, size_t bytes_recvd);

	void rpcAppend(int id, const RpcRequest &data);
	void rpcAppendResponse(int id, const RpcResponse &data);
	void rpcReqVote(int id, const RpcRequest &data);
	void rpcReqVoteResponse(int id, const RpcResponse &data);

	template<typename ConstBufferSequence>
	void socketSend(const ConstBufferSequence &buffers, const boost::asio::ip::udp::endpoint &destination);
	int findMatchingAddress(const boost::asio::ip::address &addr, int &id);
	int scanLocalInterfaces();

protected:
	boost::asio::io_service                 &io_service_;
	boost::asio::ip::udp::socket            socket_;
	boost::asio::deadline_timer             election_timer_,heartbeat_timer_;
	boost::asio::deadline_timer             loyalty_agreement_timer_;
	boost::array<uint8_t,kMaxPacketLength>  packet_data_;
	boost::asio::ip::udp::endpoint          sender_endpoint_;

	std::vector<NodeInfo>                   node_;
	RaftState                               state_;
	bool                                    block_leader_promotion_;

	Options                                 opt_;
};
