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

#include "mount/client/lizardfs_c_api.h" // must be first
#include "common/richacl.h"

extern "C" {

int lzfs_int_apply_masks(liz_acl_t *lzfs_acl, uint32_t owner) {
	if (lzfs_acl) {
		try {
			((RichACL *)lzfs_acl)->applyMasks(owner);
		} catch (...) {
			return -1;
		}
	}

	return 0;
}
}
