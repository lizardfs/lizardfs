/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare,
   2013-2017 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

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

#include "common/platform.h"

#include "tools/tools_common_functions.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common/human_readable_format.h"
#include "common/mfserr.h"
#include "common/server_connection.h"
#include "protocol/cltoma.h"
#include "protocol/matocl.h"

uint8_t humode = 0;

const char *eattrtab[EATTR_BITS] = {EATTR_STRINGS};
const char *eattrdesc[EATTR_BITS] = {EATTR_DESCRIPTIONS};

void signalHandler(uint32_t job_id) {
	sigset_t set;
	uint32_t msgid = 0;
	sigemptyset(&set);
	sigaddset(&set, SIGINT);
	sigaddset(&set, SIGTERM);
	sigaddset(&set, SIGHUP);
	sigaddset(&set, SIGUSR1);
	int sig;
	sigwait(&set, &sig);
	if (sig == SIGINT || sig == SIGTERM || sig == SIGHUP) {
		uint32_t inode;
		int fd = open_master_conn(".", &inode, nullptr, 0, 0);
		if (fd < 0) {
			printf("Connection to master failed\n");
			return;
		}
		try {
			auto request = cltoma::stopTask::build(msgid, job_id);
			auto response = ServerConnection::sendAndReceive(fd, request,
					LIZ_MATOCL_STOP_TASK);
			uint8_t status;
			matocl::stopTask::deserialize(response, msgid, status);
			if (status == LIZARDFS_STATUS_OK) {
				printf("Task has been cancelled\n");
			} else {
				printf("Task could not be found\n");
			}
			close_master_conn(0);
		} catch (Exception &e) {
			fprintf(stderr, "%s\n", e.what());
			close_master_conn(1);
			return;
		}
	}
}

bool check_usage(std::function<void()> f, bool expressionExpectedToBeFalse, const char *format, ...) {
	if (expressionExpectedToBeFalse) {
		va_list args;
		va_start(args, format);
		vfprintf(stderr, format, args);
		va_end(args);
		f();
		return true;
	}
	return false;
}

void set_humode() {
	char *hrformat;
	hrformat = getenv("MFSHRFORMAT");
	if (hrformat) {
		if (hrformat[0] >= '0' && hrformat[0] <= '4') {
			humode = hrformat[0] - '0';
		}
		if (hrformat[0] == 'h') {
			if (hrformat[1] == '+') {
				humode = 3;
			} else {
				humode = 1;
			}
		}
		if (hrformat[0] == 'H') {
			if (hrformat[1] == '+') {
				humode = 4;
			} else {
				humode = 2;
			}
		}
	}
}

void print_number(const char *prefix, const char *suffix, uint64_t number, uint8_t mode32,
				  uint8_t bytesflag, uint8_t dflag) {
	if (prefix) {
		printf("%s", prefix);
	}
	if (dflag) {
		if (humode > 0) {
			if (bytesflag) {
				if (humode == 1 || humode == 3) {
					printf("%5sB", convertToIec(number).data());
				} else {
					printf("%4sB", convertToSi(number).data());
				}
			} else {
				if (humode == 1 || humode == 3) {
					printf(" %5s", convertToIec(number).data());
				} else {
					printf(" %4s", convertToSi(number).data());
				}
			}
			if (humode > 2) {
				if (mode32) {
					printf(" (%10" PRIu32 ")", (uint32_t)number);
				} else {
					printf(" (%20" PRIu64 ")", number);
				}
			}
		} else {
			if (mode32) {
				printf("%10" PRIu32, (uint32_t)number);
			} else {
				printf("%20" PRIu64, number);
			}
		}
	} else {
		switch (humode) {
		case 0:
			if (mode32) {
				printf("         -");
			} else {
				printf("                   -");
			}
			break;
		case 1:
			printf("     -");
			break;
		case 2:
			printf("    -");
			break;
		case 3:
			if (mode32) {
				printf("                  -");
			} else {
				printf("                            -");
			}
			break;
		case 4:
			if (mode32) {
				printf("                 -");
			} else {
				printf("                           -");
			}
			break;
		}
	}
	if (suffix) {
		printf("%s", suffix);
	}
}

int my_get_number(const char *str, uint64_t *ret, double max, uint8_t bytesflag) {
	uint64_t val, frac, fracdiv;
	double drval, mult;
	int f;
	val = 0;
	frac = 0;
	fracdiv = 1;
	f = 0;
	while (*str >= '0' && *str <= '9') {
		f = 1;
		val *= 10;
		val += (*str - '0');
		str++;
	}
	if (*str == '.') {  // accept ".5" (without 0)
		str++;
		while (*str >= '0' && *str <= '9') {
			fracdiv *= 10;
			frac *= 10;
			frac += (*str - '0');
			str++;
		}
		if (fracdiv == 1) {  // if there was '.' expect number afterwards
			return -1;
		}
	} else if (f == 0) {  // but not empty string
		return -1;
	}
	if (str[0] == '\0' || (bytesflag && str[0] == 'B' && str[1] == '\0')) {
		mult = 1.0;
	} else if (str[0] != '\0' &&
	           (str[1] == '\0' || (bytesflag && str[1] == 'B' && str[2] == '\0'))) {
		switch (str[0]) {
		case 'k':
			mult = 1e3;
			break;
		case 'M':
			mult = 1e6;
			break;
		case 'G':
			mult = 1e9;
			break;
		case 'T':
			mult = 1e12;
			break;
		case 'P':
			mult = 1e15;
			break;
		case 'E':
			mult = 1e18;
			break;
		default:
			return -1;
		}
	} else if (str[0] != '\0' && str[1] == 'i' &&
	           (str[2] == '\0' || (bytesflag && str[2] == 'B' && str[3] == '\0'))) {
		switch (str[0]) {
		case 'K':
			mult = 1024.0;
			break;
		case 'M':
			mult = 1048576.0;
			break;
		case 'G':
			mult = 1073741824.0;
			break;
		case 'T':
			mult = 1099511627776.0;
			break;
		case 'P':
			mult = 1125899906842624.0;
			break;
		case 'E':
			mult = 1152921504606846976.0;
			break;
		default:
			return -1;
		}
	} else {
		return -1;
	}
	drval = round(((double)frac / (double)fracdiv + (double)val) * mult);
	if (drval > max) {
		return -2;
	} else {
		*ret = drval;
	}
	return 1;
}

int bsd_basename(const char *path, char *bname) {
	const char *endp, *startp;

	/* Empty or NULL string gets treated as "." */
	if (path == NULL || *path == '\0') {
		(void)strcpy(bname, ".");
		return 0;
	}

	/* Strip trailing slashes */
	endp = path + strlen(path) - 1;
	while (endp > path && *endp == '/') {
		endp--;
	}

	/* All slashes becomes "/" */
	if (endp == path && *endp == '/') {
		(void)strcpy(bname, "/");
		return 0;
	}

	/* Find the start of the base */
	startp = endp;
	while (startp > path && *(startp - 1) != '/') {
		startp--;
	}

	if (endp - startp + 2 > PATH_MAX) {
		return -1;
	}

	(void)strncpy(bname, startp, endp - startp + 1);
	bname[endp - startp + 1] = '\0';
	return 0;
}

int bsd_dirname(const char *path, char *bname) {
	const char *endp;

	/* Empty or NULL string gets treated as "." */
	if (path == NULL || *path == '\0') {
		(void)strcpy(bname, ".");
		return 0;
	}

	/* Strip trailing slashes */
	endp = path + strlen(path) - 1;
	while (endp > path && *endp == '/') {
		endp--;
	}

	/* Find the start of the dir */
	while (endp > path && *endp != '/') {
		endp--;
	}

	/* Either the dir is "/" or there are no slashes */
	if (endp == path) {
		(void)strcpy(bname, *endp == '/' ? "/" : ".");
		return 0;
	} else {
		do {
			endp--;
		} while (endp > path && *endp == '/');
	}

	if (endp - path + 2 > PATH_MAX) {
		return -1;
	}
	(void)strncpy(bname, path, endp - path + 1);
	bname[endp - path + 1] = '\0';
	return 0;
}

void dirname_inplace(char *path) {
	char *endp;

	if (path == NULL) {
		return;
	}
	if (path[0] == '\0') {
		path[0] = '.';
		path[1] = '\0';
		return;
	}

	/* Strip trailing slashes */
	endp = path + strlen(path) - 1;
	while (endp > path && *endp == '/') {
		endp--;
	}

	/* Find the start of the dir */
	while (endp > path && *endp != '/') {
		endp--;
	}

	if (endp == path) {
		if (path[0] == '/') {
			path[1] = '\0';
		} else {
			path[0] = '.';
			path[1] = '\0';
		}
		return;
	} else {
		*endp = '\0';
	}
}
