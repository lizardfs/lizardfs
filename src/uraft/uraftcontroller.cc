#include "common/platform.h"
#include "uraftcontroller.h"

#include <poll.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <syslog.h>
#include <unistd.h>
#include <cstdio>
#include <iostream>
#include <stdexcept>

#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/version.hpp>

#include "common/time_utils.h"

uRaftController::uRaftController(boost::asio::io_service &ios)
	: uRaftStatus(ios),
	  check_cmd_status_timer_(ios),
	  check_node_status_timer_(ios),
	  cmd_timeout_timer_(ios) {
	command_pid_  = -1;
	command_type_ = kCmdNone;
	force_demote_ = false;
	node_alive_   = false;

	opt_.check_node_status_period = 250;
	opt_.check_cmd_status_period  = 100;
	opt_.getversion_timeout       = 50;
	opt_.promote_timeout          = 1000000;
	opt_.demote_timeout           = 1000000;
}

uRaftController::~uRaftController() {
}

void uRaftController::init() {
	uRaftStatus::init();

	set_block_promotion(true);
	if (opt_.elector_mode) {
		return;
	}

	check_cmd_status_timer_.expires_from_now(boost::posix_time::millisec(opt_.check_cmd_status_period));
	check_cmd_status_timer_.async_wait(boost::bind(&uRaftController::checkCommandStatus, this,
	                                   boost::asio::placeholders::error));

	check_node_status_timer_.expires_from_now(boost::posix_time::millisec(opt_.check_node_status_period));
	check_node_status_timer_.async_wait(boost::bind(&uRaftController::checkNodeStatus, this,
	                                    boost::asio::placeholders::error));

	syslog(LOG_NOTICE, "Lizardfs-uraft initialized properly");
}

void uRaftController::set_options(const uRaftController::Options &opt) {
	uRaftStatus::set_options(opt);
	opt_ = opt;
}

void uRaftController::nodePromote() {
	syslog(LOG_NOTICE, "Starting metadata server switch to master mode");

	if (command_pid_ >= 0 && command_type_ != kCmdPromote) {
		syslog(LOG_ERR, "Trying to switch metadata server to master during switch to slave");
		demoteLeader();
		set_block_promotion(true);
		return;
	}
	if (command_pid_ >= 0) {
		return;
	}

	setSlowCommandTimeout(opt_.promote_timeout);
	if (runSlowCommand("lizardfs-uraft-helper promote")) {
		command_type_ = kCmdPromote;
	}
}

void uRaftController::nodeDemote() {
	syslog(LOG_NOTICE, "Starting metadata server switch to slave mode");

	if (command_pid_ >= 0 && command_type_ != kCmdDemote) {
		syslog(LOG_ERR, "Trying to switch metadata server to slave during switch to master");
		force_demote_ = true;
		set_block_promotion(true);
		return;
	}
	if (command_pid_ >= 0) {
		return;
	}

	setSlowCommandTimeout(opt_.demote_timeout);
	if (runSlowCommand("lizardfs-uraft-helper demote")) {
		command_type_ = kCmdDemote;
		set_block_promotion(true);
	}
}

uint64_t uRaftController::nodeGetVersion() {
	if (opt_.elector_mode) {
		return 0;
	}

	uint64_t    res;

	try {
		std::string version;

		std::vector<std::string> params = {
			"lizardfs-uraft-helper", "metadata-version", opt_.local_master_server,
			boost::lexical_cast<std::string>(opt_.local_master_port)
		};

		if (!runCommand(params, version, opt_.getversion_timeout)) {
			syslog(LOG_WARNING, "Get metadata version timeout.");
			return state_.data_version;
		}

		res = boost::lexical_cast<uint64_t>(version.c_str());
	} catch (...) {
		syslog(LOG_ERR, "Invalid metadata version value.");
		res = state_.data_version;
	}

	return res;
}

void uRaftController::nodeLeader(int id) {
	if (id < 0) {
		return;
	}

	std::string name = opt_.server[id];
	std::string::size_type p = name.find(":");

	if (p != std::string::npos) {
		name = name.substr(0, p);
	}

	syslog(LOG_NOTICE, "Node '%s' is now a leader.", name.c_str());
}

/*! \brief Check promote/demote script status. */
void uRaftController::checkCommandStatus(const boost::system::error_code &error) {
	if (error) return;

	int  status;
	if (checkSlowCommand(status)) {
		cmd_timeout_timer_.cancel();
		if (command_type_ == kCmdDemote) {
			syslog(LOG_NOTICE, "Metadata server switch to slave mode done");
			command_type_ = kCmdNone;
			command_pid_  = -1;
			set_block_promotion(false);
		}
		if (command_type_ == kCmdPromote) {
			syslog(LOG_NOTICE, "Metadata server switch to master mode done");
			node_alive_ = true;
			command_type_ = kCmdNone;
			command_pid_  = -1;
			if (force_demote_) {
				syslog(LOG_WARNING, "Staring forced switch to slave mode");
				nodeDemote();
				force_demote_ = false;
			}
		}
	}

	check_cmd_status_timer_.expires_from_now(boost::posix_time::millisec(opt_.check_cmd_status_period));
	check_cmd_status_timer_.async_wait(boost::bind(&uRaftController::checkCommandStatus, this,
	                                   boost::asio::placeholders::error));
}

/*! \brief Check metadata server status. */
void uRaftController::checkNodeStatus(const boost::system::error_code &error) {
	if (error) return;

	std::vector<std::string> params = { "lizardfs-uraft-helper", "isalive" };
	std::string              result;
	bool                     is_alive = node_alive_;

	if (command_type_ == kCmdNone) {
		if (runCommand(params, result, opt_.getversion_timeout)) {
			if (result == "alive" || result == "dead") {
				is_alive = result == "alive";
			} else {
				syslog(LOG_ERR, "Invalid metadata server status.");
			}
		}

		if (is_alive != node_alive_) {
			if (is_alive) {
				syslog(LOG_NOTICE, "Metadata server is alive");
				set_block_promotion(false);
			} else {
				syslog(LOG_NOTICE, "Metadata server is dead");
				demoteLeader();
				set_block_promotion(true);
				if (runSlowCommand("lizardfs-uraft-helper dead")) {
					command_type_ = kCmdStatusDead;
				}
			}
			node_alive_ = is_alive;
		}
	}

	check_node_status_timer_.expires_from_now(boost::posix_time::millisec(opt_.check_node_status_period));
	check_node_status_timer_.async_wait(boost::bind(&uRaftController::checkNodeStatus, this,
	                                    boost::asio::placeholders::error));
}

void uRaftController::setSlowCommandTimeout(int timeout) {
	cmd_timeout_timer_.expires_from_now(boost::posix_time::millisec(timeout));
	cmd_timeout_timer_.async_wait([this](const boost::system::error_code & error) {
		if (!error) {
			syslog(LOG_ERR, "Metadata server mode switching timeout");
			stopSlowCommand();
		}
	});
}

//! Check if slow command stopped working.
bool uRaftController::checkSlowCommand(int &status) {
	if (command_pid_ < 0) {
		return false;
	}
	return waitpid(command_pid_, &status, WNOHANG) > 0;
}

//! Kills slow command.
bool uRaftController::stopSlowCommand() {
	if (command_pid_ < 0) {
		return false;
	}

	int status;
	kill(command_pid_, SIGKILL);
	waitpid(command_pid_, &status, 0);

	command_pid_  = -1;
	command_type_ = kCmdNone;

	return true;
}

/*! \brief Start new program.
 *
 * \param cmd String with name and parameters of program to run.
 * \return true if there was no error.
 */
bool uRaftController::runSlowCommand(const std::string &cmd) {
	command_timer_.reset();

#if (BOOST_VERSION >= 104700)
	io_service_.notify_fork(boost::asio::io_service::fork_prepare);
#endif

	command_pid_ = fork();
	if (command_pid_ == -1) {
		return false;
	}
	if (command_pid_ == 0) {
		execlp("/bin/sh", "/bin/sh", "-c", cmd.c_str(), NULL);
		exit(1);
	}

#if (BOOST_VERSION >= 104700)
	io_service_.notify_fork(boost::asio::io_service::fork_parent);
#endif

	return true;
}

/*! \brief Start new program.
 *
 * \param cmd vector of string with name and parameters of program to run.
 * \param result string with the data that was written to stdout by program.
 * \param timeout time in ms after which the program will be killed.
 * \return true if there was no error and program did finish in timeout time.
 */
bool uRaftController::runCommand(const std::vector<std::string> &cmd, std::string &result, int timeout) {
	pid_t pid;
	int   pipe_fd[2];

	if (pipe(pipe_fd) == -1) {
		return false;
	}

#if (BOOST_VERSION >= 104700)
	io_service_.notify_fork(boost::asio::io_service::fork_prepare);
#endif

	pid = fork();
	if (pid == -1) {
		close(pipe_fd[0]);
		close(pipe_fd[1]);
		return false;
	}

	if (pid == 0) {
		close(pipe_fd[0]);
		dup2(pipe_fd[1], 1);

		std::vector<const char *> argv(cmd.size() + 1, 0);
		for (int i = 0; i < (int)cmd.size(); i++) {
			argv[i] = cmd[i].c_str();
		}

		execvp(argv[0], (char * const *)&argv[0]);
		exit(1);
	}

#if (BOOST_VERSION >= 104700)
	io_service_.notify_fork(boost::asio::io_service::fork_parent);
#endif

	close(pipe_fd[1]);

	int r = readString(pipe_fd[0], result, timeout);

	close(pipe_fd[0]);

	if (r <= 0) {
		kill(pid, SIGKILL);
	}

	int status;
	waitpid(pid, &status, 0);

	return r > 0;
}

/*! Read string from file descriptor
 *
 * Reads data from file descriptor and store them in string (with timeout).
 * \param fd file descriptor to read
 * \param result string with read data.
 * \param timoeut time (ms) after which we stop reading data.
 * \return -1 error
 *         0  timeout did occur
 *         1  no error
 */
int uRaftController::readString(int fd, std::string &result, const int timeout) {
	static const int read_size = 128;

	Timeout tm {std::chrono::milliseconds(timeout)};
	char    buff[read_size + 1];
	pollfd  pdata;

	pdata.fd      = fd;
	pdata.events  = POLLIN;
	pdata.revents = 0;

	result.clear();

	while (1) {
		int     r;

		if (tm.expired()) {
			return 0;
		}

		r = poll(&pdata, 1, tm.remaining_ms());
		if (r <= 0) return r;

		r = read(fd, buff, read_size);
		if (r < 0) {
			return -1;
		}
		if (r == 0) {
			break;
		}

		buff[r] = 0;
		result += buff;
	}

	return 1;
}
