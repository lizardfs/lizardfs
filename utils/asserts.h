/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <cassert>
#include <cstdio>
#include <cstdlib>

/* Check if condition if true; if no -- exit */
#define utils_massert(condition) do { \
			if(!(condition)) { \
				fprintf(stderr, "Assertion %s failed.\n", #condition); \
				exit(1); \
			} \
		} while(0)

/* Unrecoverable failure */
#define utils_mabort(msg) do { \
			fprintf(stderr, "Failure: %s\n", #msg); \
			exit(1); \
		} while(0)

/* Check if condition if true; if no -- perror and exit */
#define utils_passert(condition) do { \
			if(!(condition)) { \
				perror("Assertion " #condition " failed"); \
				exit(1); \
			} \
	} while(0)

/* Check if expression is equal to 0; if no -- perror and exit */
#define utils_zassert(expression) do { \
			if((expression) != 0) { \
				perror(#expression); \
				exit(1);\
			} \
	} while(0)
