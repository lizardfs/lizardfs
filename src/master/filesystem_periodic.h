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

/*
 * A function that is called every main loop iteration,
 * recalculates checksums in the backround background using gChecksumBackgroundUpdater
 * Recalculated checksums are FsNodesChecksum, FsEdgesChecksum, XattrChecksum and ChunksChecksum.
 * ChunksChecksum is recalculated externally using chunks_update_checksum_a_bit().
 */
void fs_background_checksum_recalculation_a_bit();
void fs_periodic_test_files();
void fs_periodic_emptytrash(void);
void fs_periodic_emptyreserved(void);
