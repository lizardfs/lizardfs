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

#include "common/platform.h"
#include "master/filesystem_checksum_background_updater.h"

#include "common/lizardfs_version.h"
#include "master/filesystem_checksum.h"
#include "master/filesystem_metadata.h"
#include "master/filesystem_operations.h"
#include "master/personality.h"

ChecksumBackgroundUpdater::ChecksumBackgroundUpdater()
	: speedLimit_(0) {  // Not important, redefined in fs_read_config_file()
	reset();
}

bool ChecksumBackgroundUpdater::start() {
	lzfs_silent_syslog(LOG_DEBUG, "master.fs.checksum.updater_start");
	if (step_ == ChecksumRecalculatingStep::kNone) {
		++step_;
		return true;
	} else {
		return false;
	}
}

void ChecksumBackgroundUpdater::end() {
	updateChecksum();
	reset();
	lzfs_silent_syslog(LOG_DEBUG, "master.fs.checksum.updater_end");
}

bool ChecksumBackgroundUpdater::inProgress() {
	return step_ != ChecksumRecalculatingStep::kNone;
}

ChecksumRecalculatingStep ChecksumBackgroundUpdater::getStep() {
	return step_;
}

void ChecksumBackgroundUpdater::incStep() {
	++step_;
	position_ = 0;
}

int32_t ChecksumBackgroundUpdater::getPosition() {
	return position_;
}

void ChecksumBackgroundUpdater::incPosition() {
	++position_;
}

bool ChecksumBackgroundUpdater::isNodeIncluded(FSNode *node) {
	auto ret = false;
	if (step_ > ChecksumRecalculatingStep::kNodes) {
		ret = true;
	}
	if (step_ == ChecksumRecalculatingStep::kNodes && NODEHASHPOS(node->id) < position_) {
		ret = true;
	}
	if (ret) {
		lzfs_silent_syslog(LOG_DEBUG, "master.fs.checksum.changing_recalculated_node");
	} else {
		lzfs_silent_syslog(LOG_DEBUG, "master.fs.checksum.changing_not_recalculated_node");
	}
	return ret;
}

bool ChecksumBackgroundUpdater::isXattrIncluded(xattr_data_entry *xde) {
	auto ret = false;
	if (step_ > ChecksumRecalculatingStep::kXattrs) {
		ret = true;
	}
	if (step_ == ChecksumRecalculatingStep::kXattrs &&
	    xattr_data_hash_fn(xde->inode, xde->anleng, xde->attrname) < position_) {
		ret = true;
	}
	if (ret) {
		lzfs_silent_syslog(LOG_DEBUG, "master.fs.checksum.changing_recalculated_xattr");
	} else {
		lzfs_silent_syslog(LOG_DEBUG, "master.fs.checksum.changing_not_recalculated_xattr");
	}
	return ret;
}

void ChecksumBackgroundUpdater::setSpeedLimit(uint32_t value) {
	speedLimit_ = value;
}

uint32_t ChecksumBackgroundUpdater::getSpeedLimit() {
	return speedLimit_;
}

void ChecksumBackgroundUpdater::updateChecksum() {
	if (fsNodesChecksum != gMetadata->fsNodesChecksum) {
		lzfs_pretty_syslog(LOG_WARNING, "FsNodes checksum mismatch found, replacing with a new value.");
		gMetadata->fsNodesChecksum = fsNodesChecksum;
		lzfs_silent_syslog(LOG_DEBUG, "master.fs.checksum.mismatch");
	}
	if (xattrChecksum != gMetadata->xattrChecksum) {
		lzfs_pretty_syslog(LOG_WARNING, "Xattr checksum mismatch found, replacing with a new value.");
		gMetadata->xattrChecksum = xattrChecksum;
		lzfs_silent_syslog(LOG_DEBUG, "master.fs.checksum.mismatch");
	}
}

void ChecksumBackgroundUpdater::reset() {
	position_ = 0;
	step_ = ChecksumRecalculatingStep::kNone;
	fsNodesChecksum = NODECHECKSUMSEED;
	xattrChecksum = XATTRCHECKSUMSEED;
}
