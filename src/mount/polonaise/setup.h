#pragma once
#include "common/platform.h"

#include <string>

#include "common/MFSCommunication.h"

struct Setup {
	std::string master_host;
	std::string master_port;
	int bind_port;
	std::string mountpoint;
	std::string password;
	uint32_t io_retries;
	uint32_t write_buffer_size;
	uint32_t report_reserved_period;
	bool forget_password;
	std::string subfolder;
	bool debug;
	double direntry_cache_timeout;
	double entry_cache_timeout;
	double attr_cache_timeout;
	bool no_mkdir_copy_sgid;
	SugidClearMode sugid_clear_mode;
#ifdef _WIN32
	std::string pipe_name;
#endif
};

extern Setup gSetup;
