#include "common/platform.h"
#include "uraftcontroller.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <syslog.h>
#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iostream>

#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options.hpp>
#include <boost/version.hpp>

void parseOptions(int argc, char **argv, uRaftController::Options &opt, bool &make_daemon, std::string &pidfile) {
	namespace po = boost::program_options;
	po::options_description generic("options");

	generic.add_options()
	("help", "produce help message")
	("config,c", po::value<std::string>()->default_value(ETC_PATH "/lizardfs-uraft.cfg"), "configuration file");

	po::options_description config("Configuration");
	config.add_options()
	("id", po::value<int>(), "server id")
	("start-daemon,d", po::bool_switch()->default_value(false), "start in daemon mode")
	("pidfile,p", po::value<std::string>(), "pidfile name");

	po::options_description hidden;
	hidden.add_options()
	("URAFT_ID", po::value<int>()->default_value(-1), "node id")
	("URAFT_PORT", po::value<int>()->default_value(9427), "node port")
	("URAFT_NODE_ADDRESS", po::value<std::vector<std::string> >(), "node address")
	("ELECTION_TIMEOUT_MIN", po::value<int>()->default_value(400), "election min timeout (ms)")
	("ELECTION_TIMEOUT_MAX", po::value<int>()->default_value(600), "election max timeout (ms)")
	("HEARTBEAT_PERIOD", po::value<int>()->default_value(20), "heartbeat period (ms)")
	("LOCAL_MASTER_ADDRESS", po::value<std::string>()->default_value("localhost"), "local master address")
	("LOCAL_MASTER_MATOCL_PORT", po::value<int>()->default_value(9421), "local master matocl port")
	("LOCAL_MASTER_CHECK_PERIOD", po::value<int>()->default_value(250), "local master check status period")
	("URAFT_ELECTOR_MODE", po::value<int>()->default_value(0), "run in elector mode")
	("URAFT_GETVERSION_TIMEOUT", po::value<int>()->default_value(50), "getversion timeout (ms)")
	("URAFT_PROMOTE_TIMEOUT", po::value<int>()->default_value(1000000000), "promote timeout (ms)")
	("URAFT_DEMOTE_TIMEOUT", po::value<int>()->default_value(1000000000), "demote timeout (ms)")
	("URAFT_DEAD_HANDLER_TIMEOUT", po::value<int>()->default_value(1000000000), "metadata server dead handler timeout (ms)")
	("URAFT_CHECK_CMD_PERIOD", po::value<int>()->default_value(100), "check command status period(ms)")
	("URAFT_STATUS_PORT", po::value<int>()->default_value(9428), "node status port");

	po::options_description cmdline_options;
	cmdline_options.add(generic).add(config).add(hidden);

	po::options_description config_file_options;
	config_file_options.add(hidden);

	po::options_description visible("Allowed options");
	visible.add(generic).add(config);

	po::positional_options_description p;
	p.add("URAFT_NODE_ADDRESS", -1);

	po::variables_map vm;
	po::store(po::command_line_parser(argc, argv).
	          options(cmdline_options).positional(p).run(), vm);
	po::notify(vm);

	if (vm.count("help")) {
		std::cout << visible << "\n";
		exit(EXIT_FAILURE);
	}

	if (vm.count("config")) {
		std::string config_file;

		config_file = vm["config"].as<std::string>();
		std::ifstream ifs(config_file.c_str());

		if (!ifs) {
			syslog(LOG_ERR, "Can not open configuration file: %s", config_file.c_str());
			std::cout << "Can not open configuration file: " << config_file << "\n";
			exit(EXIT_FAILURE);
		}

		po::store(po::parse_config_file(ifs, config_file_options, true), vm);
		po::notify(vm);
	}

	if (!vm.count("URAFT_NODE_ADDRESS")) {
		syslog(LOG_ERR, "Missing node address list");
		std::cout << visible << "\n";
		exit(EXIT_FAILURE);
	}

	opt.id                        = vm["URAFT_ID"].as<int>();
	opt.port                      = vm["URAFT_PORT"].as<int>();
	opt.server                    = vm["URAFT_NODE_ADDRESS"].as< std::vector< std::string > >();
	opt.election_timeout_min      = vm["ELECTION_TIMEOUT_MIN"].as<int>();
	opt.election_timeout_max      = vm["ELECTION_TIMEOUT_MAX"].as<int>();
	opt.heartbeat_period          = vm["HEARTBEAT_PERIOD"].as<int>();
	opt.check_node_status_period  = vm["LOCAL_MASTER_CHECK_PERIOD"].as<int>();
	opt.status_port               = vm["URAFT_STATUS_PORT"].as<int>();
	opt.elector_mode              = vm["URAFT_ELECTOR_MODE"].as<int>();
	opt.getversion_timeout        = vm["URAFT_GETVERSION_TIMEOUT"].as<int>();
	opt.promote_timeout           = vm["URAFT_PROMOTE_TIMEOUT"].as<int>();
	opt.demote_timeout            = vm["URAFT_DEMOTE_TIMEOUT"].as<int>();
	opt.dead_handler_timeout      = vm["URAFT_DEAD_HANDLER_TIMEOUT"].as<int>();
	opt.local_master_server       = vm["LOCAL_MASTER_ADDRESS"].as<std::string>();
	opt.local_master_port         = vm["LOCAL_MASTER_MATOCL_PORT"].as<int>();
	opt.check_cmd_status_period   = vm["URAFT_CHECK_CMD_PERIOD"].as<int>();
	make_daemon                   = vm["start-daemon"].as<bool>();

	if (vm.count("id")) {
		opt.id = vm["id"].as<int>();
	}

	if (opt.id >= (int)opt.server.size()) {
		syslog(LOG_ERR, "Invalid node id");
		std::cout << visible << "\n";
		exit(EXIT_FAILURE);
	}

	if (vm.count("pidfile")) {
		pidfile = vm["pidfile"].as<std::string>();
	}
}

int getSeed() {
	int r, fd, result;

	r  = -1;
	fd = open("/dev/urandom", O_RDONLY);
	if (fd >= 0) {
		r = read(fd, &result, sizeof(result));
	}

	if (r != sizeof(result)) {
		result = std::time(nullptr);
	}

	if (fd >= 0) {
		close(fd);
	}

	return result;
}

bool daemonize() {
	pid_t pid;

	pid = fork();
	if (pid) {
		if (pid > 0) {
			exit(0);
		}

		syslog(LOG_ERR, "First fork failed: %s", strerror(errno));
		return false ;
	}

	setsid();
	int r = chdir("/");
	if (r < 0) {
		syslog(LOG_ERR, "Change directory failed: %s", strerror(errno));
	}
	umask(0);

	pid = fork();
	if (pid) {
		if (pid > 0) {
			exit(0);
		}

		syslog(LOG_ERR, "Second fork failed: %s", strerror(errno));
		return false;
	}

	close(0);
	close(1);
	close(2);

	if (open("/dev/null", O_RDONLY) < 0) {
		syslog(LOG_ERR, "Unable to open /dev/null: %s", strerror(errno));
		return false;
	}
	if (open("/dev/null", O_WRONLY) < 0) {
		syslog(LOG_ERR, "Unable to open /dev/null: %s", strerror(errno));
		return false;
	}
	if (dup(1) < 0) {
		syslog(LOG_ERR, "Unable to duplicate stdout descriptor: %s", strerror(errno));
		return false;
	}

	return true;
}

void makePidFile(const std::string &name) {
	if (name.empty()) return;

	std::ofstream ofs(name, std::ios_base::out | std::ios_base::trunc);
	ofs << boost::lexical_cast<std::string>(getpid()) << std::endl;
}

int main(int argc, char **argv) {

	uRaftController::Options opt;
	bool                     make_daemon;
	std::string              pidfile;

	openlog("lizardfs-uraft", 0, LOG_DAEMON);

	srand(getSeed());
	parseOptions(argc, argv, opt, make_daemon, pidfile);

	if (make_daemon && !daemonize()) {
		syslog(LOG_ERR, "Unable to switch to daemon mode");
		return EXIT_FAILURE;
	}

	boost::asio::io_service  io_service;
	uRaftController          server(io_service);
#if (BOOST_VERSION >= 104700)
	boost::asio::signal_set  signals(io_service, SIGINT, SIGTERM);
#endif

	try {
		server.set_options(opt);
		makePidFile(pidfile);
#if (BOOST_VERSION >= 104700)
		signals.async_wait(boost::bind(&boost::asio::io_service::stop, &io_service));
#endif
		server.init();

		io_service.run();
	} catch (std::exception &e) {
		std::cerr << "Fatal error: " << e.what() << "\n";
		syslog(LOG_CRIT, "Fatal error: %s.", e.what());
		return EXIT_FAILURE;
	}

	return EXIT_SUCCESS;
}
