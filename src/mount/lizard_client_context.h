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

#include <cstdint>
#include <sys/types.h>
#include <protocol/cltoma.h>

namespace LizardClient {

/**
 * Class containing arguments that are passed with every request to the filesystem
 */
struct Context {
	typedef uint32_t IdType;
	typedef uint16_t MaskType;

	typedef cltoma::updateCredentials::GroupsContainer GroupsContainer;

	static constexpr IdType kIncorrectId = 0xffffffff;

	Context() : uid(kIncorrectId), gid(kIncorrectId), pid(), umask(), gids() {}

	Context(IdType uid, IdType gid, pid_t pid, MaskType umask)
			: uid(uid), gid(gid), pid(pid), umask(umask), gids(1, gid) {
	}

	Context(IdType uid, const GroupsContainer &gids, pid_t pid, MaskType umask)
		  : uid(uid), gid(0), pid(pid), umask(umask), gids(gids) {
	}

	bool isValid() const {
		return !gids.empty();
	}

	IdType uid;
	IdType gid;
	pid_t  pid; // Never sent to master so we can use local type.
	MaskType umask;
	GroupsContainer gids;
};

} // namespace LizardClient
