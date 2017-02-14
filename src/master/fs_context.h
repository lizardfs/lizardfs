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

#include <cassert>
#include <cstdint>

#include "common/small_vector.h"
#include "common/special_inode_defs.h"
#include "protocol/cltoma.h"
#include "protocol/MFSCommunication.h"
#include "master/personality.h"

/**
 * A class which represents objects describing how to perform filesystem operations
 * in the master server, shadow master and metarestore. It contains information about
 * who and when requested a change, and who performs it.
 */
class FsContext {
public:
	typedef cltoma::updateCredentials::GroupsContainer GroupsContainer;

	/**
	 * Returns object suitable for use by metarestore or the shadow master.
	 *
	 * \param ts - a timestamp of the operations made in this context
	 */
	static FsContext getForRestore(uint32_t ts) {
		return FsContext(ts, metadataserver::Personality::kShadow);
	}

	/**
	 * Returns object suitable for use by metarestore or the shadow master.
	 * It contains information about (remapped) uid and gid of the process
	 * which requested an operation.
	 *
	 * \param ts - a timestamp of the operations made in this context
	 * \param uid - uid of the user which will perform operations in this context
	 * \param gid - gid of the user which will perform operations in this context
	 */
	static FsContext getForRestoreWithUidGid(uint32_t ts, uint32_t uid, uint32_t gid) {
		return FsContext(ts, metadataserver::Personality::kShadow, uid, gid, uid, gid);
	}

	/**
	 * Returns object suitable for use by the master server which does not contain
	 * information about session.
	 *
	 * \param ts - a timestamp of the operations made in this context
	 */
	static FsContext getForMaster(uint32_t ts) {
		return FsContext(ts, metadataserver::Personality::kMaster);
	}

	/**
	 * Returns object suitable for use by the master server.
	 * It does not contains information about session, but does not contain information
	 * about the client needed to check permissions.
	 *
	 * \param ts - a timestamp of the operations made in this context
	 */
	static FsContext getForMaster(uint32_t ts, uint32_t rootinode, uint8_t sesflags) {
		return FsContext(ts, metadataserver::Personality::kMaster, rootinode, sesflags);
	}

	/**
	 * Returns object suitable for use by the master server.
	 *
	 * It contains full information: session data and and information
	 * about the client needed to check permissions.
	 *
	 * \param ts - a timestamp of the operations made in this context
	 * \param rootinode - inode of the mounted root directory (or 0 for meta-mount)
	 * \param sesflags - session flags
	 * \param uid - remapped uid of the user which will perform operations in this context
	 * \param gid - remapped gid of the user which will perform operations in this context
	 * \param auid - real uid of the user which will perform operations in this context
	 * \param agid - real gid of the user which will perform operations in this context
	 */
	static FsContext getForMasterWithSession(uint32_t ts,
			uint32_t rootinode, uint8_t sesflags,
			uint32_t uid, uint32_t gid, uint32_t auid, uint32_t agid) {
		return FsContext(ts, metadataserver::Personality::kMaster,
				rootinode, sesflags, uid, gid, auid, agid);
	}

	/**
	 * Returns object suitable for use by the master server.
	 *
	 * It contains full information: session data and and information
	 * about the client needed to check permissions.
	 *
	 * \param ts - a timestamp of the operations made in this context
	 * \param rootinode - inode of the mounted root directory (or 0 for meta-mount)
	 * \param sesflags - session flags
	 * \param uid - remapped uid of the user which will perform operations in this context
	 * \param gids - vector with remapped secondary group ids.
	 * \param auid - real uid of the user which will perform operations in this context
	 * \param agid - real gid of the user which will perform operations in this context
	 */
	static FsContext getForMasterWithSession(uint32_t ts,
			uint32_t rootinode, uint8_t sesflags,
			uint32_t uid, GroupsContainer &&gids,
			uint32_t auid, uint32_t agid) {
		return FsContext(ts, metadataserver::Personality::kMaster,
				rootinode, sesflags, uid, std::move(gids), auid, agid);
	}

	/**
	 * Returns object suitable for use by the master server.
	 *
	 * It contains full information: session data and and information
	 * about the client needed to check permissions.
	 *
	 * \param ts - a timestamp of the operations made in this context
	 * \param rootinode - inode of the mounted root directory (or 0 for meta-mount)
	 * \param sesflags - session flags
	 * \param uid - remapped uid of the user which will perform operations in this context
	 * \param gids - vector with remapped secondary group ids.
	 * \param auid - real uid of the user which will perform operations in this context
	 * \param agid - real gid of the user which will perform operations in this context
	 */
	static FsContext getForMasterWithSession(uint32_t ts,
			uint32_t rootinode, uint8_t sesflags,
			uint32_t uid, const GroupsContainer &gids,
			uint32_t auid, uint32_t agid) {
		return FsContext(ts, metadataserver::Personality::kMaster,
				rootinode, sesflags, uid, std::move(gids), auid, agid);
	}

	/**
	 * Returns true if we can check permissions in this context
	 */
	bool canCheckPermissions() const {
		return (hasSessionData_ && hasUidGidData_ && personality_ == metadataserver::Personality::kMaster);
	}

	/**
	 * Returns original (not remapped) gid.
	 */
	uint32_t agid() const {
		assert(hasUidGidData_);
		return agid_;
	}

	/**
	 * Returns original (not remapped) uid.
	 */
	uint32_t auid() const {
		assert(hasUidGidData_);
		return auid_;
	}

	/**
	 * Returns (remapped) uid.
	 */
	uint32_t uid() const {
		assert(hasUidGidData_);
		return uid_;
	}

	/**
	 * Returns (remapped) gid.
	 */
	uint32_t gid() const {
		assert(hasUidGidData_ && !gids_.empty());
		return gids_[0];
	}

	/**
	 * Returns (remapped) vector containing gid (first position) and secondary groups.
	 */
	const GroupsContainer &groups() const {
		return gids_;
	}

	bool hasGroup(uint32_t gid) const {
		return std::find(gids_.begin(), gids_.end(), gid) != gids_.end();
	}

	/**
	 * Returns true if rootinode and sesflags are set.
	 */
	bool hasSessionData() const {
		return hasSessionData_;
	}

	/**
	 * Returns true if gid, uid, auid, agid are set.
	 */
	bool hasUidGidData() const {
		return hasUidGidData_;
	}

	/**
	 * Returns true if this is context of the master server
	 */
	bool isPersonalityMaster() const {
		return personality_ == metadataserver::Personality::kMaster;
	}

	/**
	 * Returns true if this is context of metarestore or the shadow master
	 */
	bool isPersonalityShadow() const {
		return personality_ == metadataserver::Personality::kShadow;
	}

	/**
	 * Returns personality.
	 * This is a personality of the process which performs changes in this context
	 */
	metadataserver::Personality personality() const {
		return personality_;
	}

	/**
	 * Returns inode which is the root of the hierarchy.
	 * Returns 0 in case of meta session.
	 */
	uint32_t rootinode() const {
		assert(hasSessionData_);
		return rootinode_;
	}

	/**
	 * Returns session flags.
	 */
	uint8_t sesflags() const {
		assert(hasSessionData_);
		return sesflags_;
	}

	/**
	 * Returns timestap.
	 * This is a timestamp of any operations performed in this context.
	 */
	uint32_t ts() const {
		return ts_;
	}

private:
	uint32_t ts_;
	metadataserver::Personality personality_;
	bool hasSessionData_;
	uint32_t rootinode_;
	uint8_t sesflags_;
	bool hasUidGidData_;
	uint32_t uid_, auid_;
	GroupsContainer gids_;
	uint32_t agid_;

	/**
	 * Constructs object with session data and uid/gid data.
	 */
	FsContext(uint32_t ts, metadataserver::Personality personality,
			uint32_t rootinode, uint8_t sesflags,
			uint32_t uid, GroupsContainer &&gids, uint32_t auid, uint32_t agid)
			: ts_(ts),
			  personality_(personality),
			  hasSessionData_(true),
			  rootinode_(rootinode),
			  sesflags_(sesflags),
			  hasUidGidData_(true),
			  uid_(uid),
			  auid_(auid),
			  gids_(std::move(gids)),
			  agid_(agid) {
	}

	/**
	 * Constructs object with session data and uid/gid data.
	 */
	FsContext(uint32_t ts, metadataserver::Personality personality,
			uint32_t rootinode, uint8_t sesflags,
			uint32_t uid, const GroupsContainer &gids, uint32_t auid, uint32_t agid)
			: ts_(ts),
			  personality_(personality),
			  hasSessionData_(true),
			  rootinode_(rootinode),
			  sesflags_(sesflags),
			  hasUidGidData_(true),
			  uid_(uid),
			  auid_(auid),
			  gids_(gids),
			  agid_(agid) {
	}

	/**
	 * Constructs object with session data and uid/gid data.
	 */
	FsContext(uint32_t ts, metadataserver::Personality personality,
			uint32_t rootinode, uint8_t sesflags,
			uint32_t uid, uint32_t gid, uint32_t auid, uint32_t agid)
			: ts_(ts),
			  personality_(personality),
			  hasSessionData_(true),
			  rootinode_(rootinode),
			  sesflags_(sesflags),
			  hasUidGidData_(true),
			  uid_(uid),
			  auid_(auid),
			  gids_(1, gid),
			  agid_(agid) {
	}

	/**
	 * Constructs object with session data and without uid/gid data.
	 */
	FsContext(uint32_t ts, metadataserver::Personality personality, uint32_t rootinode, uint8_t sesflags)
			: ts_(ts),
			  personality_(personality),
			  hasSessionData_(true),
			  rootinode_(rootinode),
			  sesflags_(sesflags),
			  hasUidGidData_(false),
			  uid_(0),
			  auid_(0),
			  gids_(),
			  agid_(0) {
	}

	/**
	 * Constructs object without session data and without uid/gid data.
	 */
	FsContext(uint32_t ts, metadataserver::Personality personality)
			: ts_(ts),
			  personality_(personality),
			  hasSessionData_(false),
			  rootinode_(SPECIAL_INODE_ROOT),
			  sesflags_(0),
			  hasUidGidData_(false),
			  uid_(0),
			  auid_(0),
			  gids_(),
			  agid_(0) {
	}

	/**
	 * Constructs object without session data and with uid/gid data.
	 */
	FsContext(uint32_t ts, metadataserver::Personality personality,
			uint32_t uid, uint32_t gid, uint32_t auid, uint32_t agid)
			: ts_(ts),
			  personality_(personality),
			  hasSessionData_(false),
			  rootinode_(SPECIAL_INODE_ROOT),
			  sesflags_(0),
			  hasUidGidData_(true),
			  uid_(uid),
			  auid_(auid),
			  gids_(1, gid),
			  agid_(agid) {
	}
};

