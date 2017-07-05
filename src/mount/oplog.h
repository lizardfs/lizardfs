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

#include <inttypes.h>
#include <mount/lizard_client_context.h>

#ifndef __printflike
#ifdef __GNUC__
#define __printflike(fmt,va1) __attribute__((__format__(printf, fmt, va1)))
#else
#define __printflike(fmt, va1)
#endif
#endif /* __printflike */

void oplog_printf(
		const struct LizardClient::Context &ctx,const char *format,...) __printflike(2, 3);
void oplog_printf(const char *format,...) __printflike(1, 2);
unsigned long oplog_newhandle(int hflag);
void oplog_releasehandle(unsigned long fh);
void oplog_getdata(unsigned long fh,uint8_t **buff,uint32_t *leng,uint32_t maxleng);
void oplog_releasedata(unsigned long fh);
