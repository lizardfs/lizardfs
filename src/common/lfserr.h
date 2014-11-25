/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013 Skytechnology sp. z o.o..

   This file was part of LizardFS and is part of LizardFS.

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

#include <inttypes.h>

#include "common/LFSCommunication.h"

/// returns errno string representation
const char* strerr(int error);
void strerr_init();
void strerr_term();

/// converts lfs error to errno
int lfs_errorconv(uint8_t status);

/// returns lfs error string representation
static inline const char* lfsstrerr(uint8_t status) {
	static const char* errtab[]={ERROR_STRINGS};
	if (status>ERROR_MAX) {
		status=ERROR_MAX;
	}
	return errtab[status];
}
