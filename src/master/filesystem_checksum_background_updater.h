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

#include "master/filesystem_node.h"
#include "master/filesystem_xattr.h"

/*!
 * \brief Steps during recalculating checksum in the background
 * It is essential that kNone is at the beginning, and kDone at the end.
 */
enum class ChecksumRecalculatingStep {
	kNone,
	kNodes,
	kXattrs,
	kChunks,
	kDone
};

// Special behavior for ++ChecksumRecalculatingStep
inline ChecksumRecalculatingStep &operator++(ChecksumRecalculatingStep &c) {
	sassert(c != ChecksumRecalculatingStep::kDone);
	c = static_cast<ChecksumRecalculatingStep>(static_cast<int>(c) + 1);
	return c;
}

/*!
 * \brief Updates checksums in the background, recalculating them from the beginning.
 *
 * Recalculation is done in steps as described in ChecksumRecalculatingStep.
 * This class holds information about the recalculation progress,
 * actual recalculating is done in function fs_background_checksum_recalculation_a_bit().
 * Controls recalculation of fsNodesChecksum, fsEdgesChecksum and xattrChecksum.
 * ChunksChecksum is recalculated externally using chunks_update_checksum_a_bit().
 */
class ChecksumBackgroundUpdater {
public:
	ChecksumBackgroundUpdater();

	// start recalculating, true if succeeds
	bool start();

	// end recalculating and update checksums if mismatch found
	void end();

	// is recalculating in progress?
	bool inProgress();

	ChecksumRecalculatingStep getStep();

	// go to next step of recalculating, resets position
	void incStep();

	int32_t getPosition();
	void incPosition();

	// is node already included in the background checksum?
	bool isNodeIncluded(FSNode *node);

	// is xattr already included in the background checksum?
	bool isXattrIncluded(xattr_data_entry *xde);

	void setSpeedLimit(uint32_t value);

	uint32_t getSpeedLimit();

	void updateChecksum();

	// prepare for next checksum recalculation
	void reset();

public:
	// Checksum values
	uint64_t fsNodesChecksum;
	uint64_t xattrChecksum;

private:
	// current step
	ChecksumRecalculatingStep step_;

	// How many objects should be processed per one
	// fs_background_checksum_recalculation_a_bit()?
	uint32_t speedLimit_;

	// current position in hashtable
	uint32_t position_;
};
