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

#include "config.h"

#include <stdio.h>

#include "topology.h"
#include "exports.h"
#include "datacachemgr.h"
#include "matomlserv.h"
#include "matocsserv.h"
#include "matoclserv.h"
#include "filesystem.h"
#include "random.h"
#include "changelog.h"
#include "chartsdata.h"
#include "masterconn.h"

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)
const char id[]="@(#) version: " STR(VERSMAJ) "." STR(VERSMID) "." STR(VERSMIN) ", written by Jakub Kruszona-Zawadzki";

/* Run Tab */
typedef int (*runfn)(void);
struct {
	runfn fn;
	char *name;
} RunTab[]={
	{changelog_init,"change log"},
	{rnd_init,"random generator"},
	{dcm_init,"data cache manager"}, // has to be before 'fs_init' and 'matoclserv_networkinit'
	{matoclserv_sessionsinit,"load stored sessions"}, // has to be before 'fs_init'
	{exports_init,"exports manager"},
	{topology_init,"net topology module"},
	{fs_init,"file system manager"},
	{chartsdata_init,"charts module"},
	{matomlserv_init,"communication with metalogger"},
	{matocsserv_init,"communication with chunkserver"},
	{matoclserv_networkinit,"communication with clients"},
	{masterconn_init,"communication with master"},
	{(runfn)0,"****"}
},LateRunTab[]={
	{(runfn)0,"****"}
};
