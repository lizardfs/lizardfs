/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare,
   2013-2017 Skytechnology sp. z o.o..

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

#pragma once

#include "common/platform.h"

#include <cstring>
#include <functional>
#include <unordered_map>
#include <string>
#include <vector>

std::function<int(int, char **)> getCommand(const std::string &name);
int printUsage(int argc = 0, char **argv = nullptr);
void printTools();
void printArgs(int argc, char **argv);

int append_file_run(int argc, char **argv);
int check_file_run(int argc, char **argv);
int dir_info_run(int argc, char **argv);
int file_info_run(int argc, char **argv);
int file_repair_run(int argc, char **argv);
int snapshot_run(int argc, char **argv);

int get_eattr_run(int argc, char **argv);
int del_eattr_run(int argc, char **argv);
int set_eattr_run(int argc, char **argv);

int get_goal_run(int argc, char **argv);
int rget_goal_run(int argc, char **argv);
int set_goal_run(int argc, char **argv);
int rset_goal_run(int argc, char **argv);

int get_trashtime_run(int argc, char **argv);
int rget_trashtime_run(int argc, char **argv);
int set_trashtime_run(int argc, char **argv);
int rset_trashtime_run(int argc, char **argv);

int quota_rep_run(int argc, char **argv);
int quota_set_run(int argc, char **argv);

int recursive_remove_run(int argc, char **argv);
