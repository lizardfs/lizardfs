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

#include "common/random.h"
#include "master/changelog.h"
#include "master/chartsdata.h"
#include "master/datacachemgr.h"
#include "master/exports.h"
#include "master/filesystem.h"
#include "master/matoclserv.h"
#include "master/matocsserv.h"
#include "master/matomlserv.h"
#include "master/personality.h"
#include "master/topology.h"
#include "metalogger/masterconn.h"

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)
const char id[]="@(#) version: " STR(VERSMAJ) "." STR(VERSMID) "." STR(VERSMIN) ", written by Jakub Kruszona-Zawadzki";

/* Run Tab */
typedef int (*runfn)(void);
struct run_tab {
	runfn fn;
	const char *name;
};

run_tab RunTab[]={
	{metadataserver::personality_init, "personality"}, // has to be first
	{changelog_init,"change log"},
	{rnd_init,"random generator"},
	{dcm_init,"data cache manager"}, // has to be before 'fs_init' and 'matoclserv_networkinit'
	{matoclserv_sessionsinit,"load stored sessions"}, // has to be before 'fs_init'
	{exports_init,"exports manager"},
	{topology_init,"net topology module"},
	{fs_init,"file system manager"},
	{chartsdata_init,"charts module"},
	{masterconn_init,"communication with master server"},
	{matomlserv_init,"communication with metalogger"},
	{matocsserv_init,"communication with chunkserver"},
	{matoclserv_networkinit,"communication with clients"},
	{(runfn)0,"****"}
},LateRunTab[]={
	{(runfn)0,"****"}
};
