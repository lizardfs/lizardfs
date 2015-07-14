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

#include "common/lizardfs_version.h"
#include "common/massert.h"
#include "master/checksum.h"
#include "master/filesystem_metadata.h"
#include "master/filesystem_node.h"
#include "master/personality.h"

void fsedges_checksum_add_to_background(fsedge *edge);
void fsedges_update_checksum(fsedge *edge);
void fsnodes_checksum_add_to_background(fsnode *node);
void fsnodes_update_checksum(fsnode *node);

uint64_t fs_checksum(ChecksumMode mode);
uint8_t fs_start_checksum_recalculation();
