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

#ifndef _MASSERT_H_
#define _MASSERT_H_

#include <errno.h>
#include <stdio.h>
#include <syslog.h>
#include <stdlib.h>

#ifdef THROW_INSTEAD_OF_ABORT
	#include <stdexcept>
	#include <string>
	#define ABORT_OR_THROW (throw std::runtime_error(std::string(__FILE__ ":") + \
			std::to_string(__LINE__)))
#else
	#define ABORT_OR_THROW abort()
#endif

#include "common/strerr.h"

#define massert(e, msg) ((e) ? (void)0 : (fprintf(stderr, "failed assertion '%s' : %s\n", #e, \
		(msg)), syslog(LOG_ERR, "failed assertion '%s' : %s", #e, (msg)), ABORT_OR_THROW))
#define passert(ptr) ((ptr != NULL) ? (void)0 : (fprintf(stderr, "out of memory: %s is NULL\n", \
		#ptr), syslog(LOG_ERR,"out of memory: %s is NULL", #ptr), ABORT_OR_THROW))
#define sassert(e) ((e) ? (void)0 : (fprintf(stderr, "failed assertion '%s'\n", #e), \
		syslog(LOG_ERR, "failed assertion '%s'", #e), ABORT_OR_THROW))
#define eassert(e) if (!(e)) { \
		const char *_mfs_errorstring = strerr(errno); \
		syslog(LOG_ERR,"failed assertion '%s', error: %s",#e,_mfs_errorstring); \
		fprintf(stderr,"failed assertion '%s', error: %s\n",#e,_mfs_errorstring); \
		ABORT_OR_THROW; \
	}
#define zassert(e) if ((e)!=0) { \
		const char *_mfs_errorstring = strerr(errno); \
		syslog(LOG_ERR,"unexpected status, '%s' returned: %s",#e,_mfs_errorstring); \
		fprintf(stderr,"unexpected status, '%s' returned: %s\n",#e,_mfs_errorstring); \
		ABORT_OR_THROW; \
	}
#define mabort(msg) do { fprintf(stderr,"abort '%s'\n", msg); syslog(LOG_ERR, "abort '%s'", msg); \
	ABORT_OR_THROW; } while (false)

#endif
