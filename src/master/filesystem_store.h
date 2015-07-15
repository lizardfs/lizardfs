/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare,
   2013-2015 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS  If not, see <http://www.gnu.org/licenses/>.
*/

#pragma once

#include "common/platform.h"

#include <cstdio>

#include "common/exceptions.h"
#include "master/metadata_dumper.h"

LIZARDFS_CREATE_EXCEPTION_CLASS(MetadataException, Exception);
LIZARDFS_CREATE_EXCEPTION_CLASS(MetadataFsConsistencyException, MetadataException);
LIZARDFS_CREATE_EXCEPTION_CLASS(MetadataConsistencyException, MetadataException);

bool fs_commit_metadata_dump();

int  fs_emergency_saves();

void fs_broadcast_metadata_saved(uint8_t status);
void fs_load_changelogs();
void fs_load_changelog(const std::string &path);
void fs_loadall(const std::string& fname,int ignoreflag);
void fs_store_fd(FILE *fd);
