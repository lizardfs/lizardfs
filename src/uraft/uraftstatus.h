#pragma once

#include "common/platform.h"

#include <list>

#include "uraft.h"

/*! \brief Helper class for uRaftStatus.
 *
 * Manages connection from client.
 */
class uRaftStatusConnection : public std::enable_shared_from_this<uRaftStatusConnection> {
public:
	uRaftStatusConnection(boost::asio::io_service &ios);

	void init();
	boost::asio::ip::tcp::socket& socket();

public:
	std::vector<uint8_t>         data_;

protected:
	boost::asio::ip::tcp::socket socket_;

};


/*! \brief uRaft status server.
 *
 * Server listen on specified tcp port and replies with information about uRaft status.
 */
class uRaftStatus : public uRaft {
public:
	struct Options : uRaft::Options {
		int status_port;
	};

public:
	uRaftStatus(boost::asio::io_service &ios);
	virtual ~uRaftStatus();

	//! Initialize server.
	void init();

	//! Set options.
	void set_options(const Options &opt);

protected:
	void acceptConnection();
	void storeData(std::vector<uint8_t> &response);

private:
	boost::asio::ip::tcp::acceptor acceptor_;
	boost::asio::ip::tcp::socket   socket_;
	Options                        opt_;
};
