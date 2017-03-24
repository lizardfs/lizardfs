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

#include <string>

#include "protocol/MFSCommunication.h"

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
	unsigned direntry_cache_size;
	double entry_cache_timeout;
	double attr_cache_timeout;
	bool no_mkdir_copy_sgid;
	SugidClearMode sugid_clear_mode;
	bool make_daemon;
	bool enable_acl;
#ifdef _WIN32
	std::string pipe_name;
#endif
};

extern Setup gSetup;
