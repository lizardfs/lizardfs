/*
   Copyright 2017 Skytechnology sp. z o.o.

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

#include "common/platform.h"
#include "admin/list_tasks_command.h"

#include <iostream>

#include "admin/registered_admin_connection.h"
#include "common/job_info.h"
#include "protocol/cltoma.h"
#include "protocol/matocl.h"

std::string ListTasksCommand::name() const {
	return "list-tasks";
}

void ListTasksCommand::usage() const {
	std::cerr << name() << " <master ip> <master port>" << std::endl;
	std::cerr << "    Lists tasks which are currently executed by master" << std::endl;
}

void ListTasksCommand::run(const Options& options) const {
	if (options.arguments().size() != 2) {
		throw WrongUsageException("Expected <master ip> and <master port> for " + name());
	}

	ServerConnection connection(options.argument(0), options.argument(1));
	std::vector<JobInfo> jobs_info;

	auto request = cltoma::listTasks::build(true);
	auto response = connection.sendAndReceive(request, LIZ_MATOCL_LIST_TASKS);
	matocl::listTasks::deserialize(response, jobs_info);
	if (jobs_info.empty()) {
		std::cout << "No tasks are being executed" << std::endl;
	}

	for (const JobInfo &job_info : jobs_info) {
		std::ios::fmtflags f(std::cout.flags());
		std::cout << "Id: 0x";
		std::cout.width(5);
		std::cout << std::left << std::hex << job_info.id << "  -  ";
		std::cout.width(15);
		std::cout << std::left << job_info.description << std::endl;
		std::cout.flags(f);
	}
}
