/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o..

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

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

#include "common/mfserr.h"
#include "common/slogger.h"

#ifdef THROW_INSTEAD_OF_ABORT
#  include <stdexcept>
#  include <string>
#  define ABORT_OR_THROW() throw std::runtime_error(\
		std::string(__FILE__ ":") + std::to_string(__LINE__))
#else
#  define ABORT_OR_THROW() abort()
#endif

#define massert(e, msg) do { if (!(e)) { \
				lzfs_pretty_syslog(LOG_ERR, "failed assertion '%s' : %s", #e, (msg)); \
				ABORT_OR_THROW(); \
		} } while (false)

#define passert(ptr) do { if ((ptr) == NULL) { \
				lzfs_pretty_syslog(LOG_ERR, "out of memory: %s is NULL", #ptr); \
				ABORT_OR_THROW(); \
		} } while (false)

#define sassert(e) do { if (!(e)) { \
				lzfs_pretty_syslog(LOG_ERR, "failed assertion '%s'", #e); \
				ABORT_OR_THROW(); \
		} } while (false)

#define eassert(e) do { if (!(e)) { \
			const char *_mfs_errorstring = strerr(errno); \
			lzfs_pretty_syslog(LOG_ERR, "failed assertion '%s', error: %s", #e, _mfs_errorstring); \
			ABORT_OR_THROW(); \
		} } while(false)

#define zassert(e) do { if ((e) != 0) { \
			const char *_mfs_errorstring = strerr(errno); \
			lzfs_pretty_syslog(LOG_ERR, "unexpected status, '%s' returned: %s", #e, _mfs_errorstring); \
			ABORT_OR_THROW(); \
		} } while(false)

#define mabort(msg) do { \
			lzfs_pretty_syslog(LOG_ERR, "abort '%s'", msg); \
			ABORT_OR_THROW(); \
		} while (false)
