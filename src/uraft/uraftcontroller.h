#pragma once

#include "common/platform.h"

#include <unistd.h>

#include "common/time_utils.h"
#include "uraftstatus.h"

/*! \brief Managament of LizardFS metadata server based on uRaft algorithm.
 *
 * This class manages local LizardFS master/shadow server. This is done using
 * base class uRaft for selection of leader. We get informed on leader change by call
 * to one of virtual node* methods. Then we call helper script to switch metadata server
 * to proper mode (master/shadow).
 *
 * class gets information from uRaft class about required state change.
 */
class uRaftController : public uRaftStatus {
public:
	enum CommandType { kCmdNone,kCmdPromote,kCmdDemote,kCmdStatusDead };

	struct Options : uRaftStatus::Options {
		std::string local_master_server;      //!< Local LizardFS master server address. //
		int         local_master_port;        //!< Local LizardFS master server matocl port. //
		int         elector_mode;             //!< Enable elector mode. //
		int         check_node_status_period; //!< How often we check master server status. //
		int         check_cmd_status_period;  //!< How often we check script status. //
		int         getversion_timeout;       //!< Time after which we kill get version script. //
		int         promote_timeout;          //!< Time after which we kill promote script. //
		int         demote_timeout;           //!< Time after which we kill demote script. //
		int         dead_handler_timeout;     //!< Time after which we kill dead script. //
	};

public:
	uRaftController(boost::asio::io_service &);
	virtual ~uRaftController();

	//! Initialize data.
	void init();

	//! Set options.
	void set_options(const Options &opt);

	//! called by uRaft when node is becoming leader.
	virtual void     nodePromote();

	//! called by uRaft when node is not longer a leader.
	virtual void     nodeDemote();

	//! called by uRaft when it needs to know metadata version.
	virtual uint64_t nodeGetVersion();

	//! called by uRaft with new leader id.
	virtual void     nodeLeader(int id);

protected:
	void  checkCommandStatus(const boost::system::error_code &error);
	void  checkNodeStatus(const boost::system::error_code &error);

	bool  runSlowCommand(const std::string &cmd);
	bool  checkSlowCommand(int &status);
	bool  stopSlowCommand();
	void  setSlowCommandTimeout(int timeout);

	bool  runCommand(const std::vector<std::string> &cmd, std::string &result, int timeout);
	int   readString(int fd, std::string &result, int timeout);

protected:
	boost::asio::deadline_timer check_cmd_status_timer_,check_node_status_timer_;
	boost::asio::deadline_timer cmd_timeout_timer_;
	pid_t                       command_pid_;
	int                         command_type_;  /// Last run command type.
	Timer                       command_timer_;
	bool                        force_demote_;
	bool                        node_alive_;  /// Last is_alive node status.
	Options                     opt_;
};
