/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA.

   This file is part of MooseFS.

   MooseFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   MooseFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with MooseFS.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _MFSSTRERR_H_
#define _MFSSTRERR_H_

#include "MFSCommunication.h"

static inline const char* mfsstrerr(uint8_t status) {
	static const char* errtab[]={ERROR_STRINGS};
	if (status>ERROR_MAX) {
		status=ERROR_MAX;
	}
	return errtab[status];
}

#endif
