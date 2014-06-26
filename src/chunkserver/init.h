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

#include "common/platform.h"

#include <stdio.h>

#include "chunkserver/chartsdata.h"
#include "chunkserver/csserv.h"
#include "chunkserver/hddspacemgr.h"
#include "chunkserver/masterconn.h"
#include "common/random.h"

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)
const char id[]="@(#) version: " STR(PACKAGE_VERSION_MAJOR) "." STR(PACKAGE_VERSION_MINOR) "." STR(PACKAGE_VERSION_MICRO) ", written by Jakub Kruszona-Zawadzki";

/* Run Tab */
typedef int (*runfn)(void);
struct run_tab {
	runfn fn;
	const char *name;
};

run_tab RunTab[]={
	{rnd_init,"random generator"},
	{hdd_init,"hdd space manager"},
	{csserv_init,"main server module"},     /* it has to be before "masterconn" */
	{masterconn_init,"master connection module"},
	{chartsdata_init,"charts module"},
	{(runfn)0,"****"}
},LateRunTab[]={
	{hdd_late_init,"hdd space manager - threads"},
	{(runfn)0,"****"}
};
