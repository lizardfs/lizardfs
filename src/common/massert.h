/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013 Skytechnology sp. z o.o..

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

#include <errno.h>
#include <stdio.h>
#include <syslog.h>
#include <stdlib.h>

#include "common/strerr.h"

#define massert(e,msg) ((e) ? (void)0 : (fprintf(stderr,"failed assertion '%s' : %s\n",#e,(msg)),syslog(LOG_ERR,"failed assertion '%s' : %s",#e,(msg)),abort()))
#define passert(ptr) ((ptr!=NULL) ? (void)0 : (fprintf(stderr,"out of memory: %s is NULL\n",#ptr),syslog(LOG_ERR,"out of memory: %s is NULL",#ptr),abort()))
#define sassert(e) ((e) ? (void)0 : (fprintf(stderr,"failed assertion '%s'\n",#e),syslog(LOG_ERR,"failed assertion '%s'",#e),abort()))
#define eassert(e) if (!(e)) { \
		const char *_mfs_errorstring = strerr(errno); \
		syslog(LOG_ERR,"failed assertion '%s', error: %s",#e,_mfs_errorstring); \
		fprintf(stderr,"failed assertion '%s', error: %s\n",#e,_mfs_errorstring); \
		abort(); \
	}
#define zassert(e) if ((e)!=0) { \
		const char *_mfs_errorstring = strerr(errno); \
		syslog(LOG_ERR,"unexpected status, '%s' returned: %s",#e,_mfs_errorstring); \
		fprintf(stderr,"unexpected status, '%s' returned: %s\n",#e,_mfs_errorstring); \
		abort(); \
	}
