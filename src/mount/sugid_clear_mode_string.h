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

#pragma once

#include "common/platform.h"
#include "protocol/MFSCommunication.h"

#include <array>

inline const char *sugidClearModeString(SugidClearMode mode) {
	static std::array<const char *, 6> sugid_strings = {{
		"NEVER", "ALWAYS", "OSX", "BSD", "EXT", "XFS"
	}};
	if ((unsigned)mode >= sugid_strings.size()) {
		return "???";
	}
	return sugid_strings[(int)mode];
}
