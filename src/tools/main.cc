/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare,
   2013-2016 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

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

#include "common/platform.h"

#include <functional>
#include <stdio.h>
#include <string>
#include <unordered_map>

#include "common/mfserr.h"
#include "tools/tools_commands.h"
#include "tools/tools_common_functions.h"

static std::unordered_map<std::string, std::function<int(int, char **)>> functions({
	{"mfsgetgoal", get_goal_run},
	{"mfsrgetgoal", rget_goal_run},
	{"mfssetgoal", set_goal_run},
	{"mfsrsetgoal", rset_goal_run},
	{"mfsgettrashtime", get_trashtime_run},
	{"mfsrgettrashtime", rget_trashtime_run},
	{"mfssettrashtime", set_trashtime_run},
	{"mfsrsettrashtime", rset_trashtime_run},
	{"mfscheckfile", check_file_run},
	{"mfsfileinfo", file_info_run},
	{"mfsappendchunks", append_file_run},
	{"mfsdirinfo", dir_info_run},
	{"mfsgeteattr", get_eattr_run},
	{"mfsseteattr", set_eattr_run},
	{"mfsdeleattr", del_eattr_run},
	{"mfsfilerepair", file_repair_run},
	{"mfsmakesnapshot", snapshot_run},
	{"mfsrepquota", quota_rep_run},
	{"mfssetquota", quota_set_run},
});

int main(int argc, char **argv) {
	int status;
	strerr_init();
	set_humode();

	std::string func_name(argv[0]);
	std::size_t found = func_name.find_last_of("/");
	if (found != std::string::npos) {
		func_name = func_name.substr(found+1);
	}
	auto func = functions.find(func_name);

	if (func == functions.end()) {
		fprintf(stderr, "unknown command: %s\n", argv[0]);
		return 1;
	} else {
		status = func->second(argc, argv);
	}
	return status;
}
