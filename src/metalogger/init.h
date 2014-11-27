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

#include "common/platform.h"

#include <stdio.h>

#include "masterconn.h"

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)

/* Run Tab */
typedef int (*runfn)(void);
struct run_tab {
	runfn fn;
	const char *name;
};

run_tab RunTab[]={
	{masterconn_init,"connection with master"},
	{(runfn)0,"****"}
},LateRunTab[]={
	{(runfn)0,"****"}
};
