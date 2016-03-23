/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare,
   2013-2016 Skytechnology sp. z o.o..

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

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <limits>
#include <sstream>
#include <vector>

#include "common/chunk_copies_calculator.h"
#include "common/chunk_with_address_and_label.h"
#include "common/goal.h"
#include "common/human_readable_format.h"
#include "common/sockets.h"
#include "common/special_inode_defs.h"

#include "tools/tools_commands.h"

int main(int argc, char **argv) {
	int l, f, status;
	int i, found;
	int ch;
	int oflag = 0;
	int rflag = 0;
	int lflag = 0;
	uint8_t eattr = 0, smode = SMODE_SET;
	std::string goal;
	uint32_t trashtime = 86400;
	char *appendfname = NULL;
	char *hrformat;
	strerr_init();

	l = strlen(argv[0]);

#define CHECKNAME(name)                                              \
	((l == (int)(sizeof(name) - 1) && strcmp(argv[0], name) == 0) || \
	 (l > (int)(sizeof(name) - 1) && strcmp((argv[0]) + (l - sizeof(name)), "/" name) == 0))

	if (CHECKNAME("mfstools")) {
		if (argc == 2 && strcmp(argv[1], "create") == 0) {
			fprintf(stderr, "create symlinks\n");
#define SYMLINK(name)                                \
	if (symlink(argv[0], name) < 0) {                \
		perror("error creating symlink '" name "'"); \
	}
			SYMLINK("mfsgetgoal")
			SYMLINK("mfssetgoal")
			SYMLINK("mfsgettrashtime")
			SYMLINK("mfssettrashtime")
			SYMLINK("mfscheckfile")
			SYMLINK("mfsfileinfo")
			SYMLINK("mfsappendchunks")
			SYMLINK("mfsdirinfo")
			SYMLINK("mfsfilerepair")
			SYMLINK("mfsmakesnapshot")
			SYMLINK("mfsgeteattr")
			SYMLINK("mfsseteattr")
			SYMLINK("mfsdeleattr")
			SYMLINK("mfsrepquota")
			SYMLINK("mfssetquota")
			// deprecated tools:
			SYMLINK("mfsrgetgoal")
			SYMLINK("mfsrsetgoal")
			SYMLINK("mfsrgettrashtime")
			SYMLINK("mfsrsettrashtime")
			return 0;
		} else {
			fprintf(stderr,
			        "mfs multi tool\n\nusage:\n\tmfstools create - create symlinks (mfs<toolname> "
			        "-> %s)\n",
			        argv[0]);
			fprintf(stderr, "\ntools:\n");
			fprintf(stderr, "\tmfsgetgoal\n\tmfssetgoal\n\tmfsgettrashtime\n\tmfssettrashtime\n");
			fprintf(stderr, "\tmfssetquota\n\tmfsrepquota\n");
			fprintf(stderr,
			        "\tmfscheckfile\n\tmfsfileinfo\n\tmfsappendchunks\n\tmfsdirinfo\n\tmfsfilerepai"
			        "r\n");
			fprintf(stderr, "\tmfsmakesnapshot\n");
			fprintf(stderr, "\tmfsgeteattr\n\tmfsseteattr\n\tmfsdeleattr\n");
			fprintf(stderr, "\ndeprecated tools:\n");
			fprintf(stderr, "\tmfsrgetgoal = mfsgetgoal -r\n");
			fprintf(stderr, "\tmfsrsetgoal = mfssetgoal -r\n");
			fprintf(stderr, "\tmfsrgettrashtime = mfsgettreshtime -r\n");
			fprintf(stderr, "\tmfsrsettrashtime = mfssettreshtime -r\n");
			return 1;
		}
	} else if (CHECKNAME("mfsgetgoal")) {
		f = MFSGETGOAL;
	} else if (CHECKNAME("mfsrgetgoal")) {
		f = MFSGETGOAL;
		rflag = 1;
		fprintf(stderr, "deprecated tool - use \"mfsgetgoal -r\"\n");
	} else if (CHECKNAME("mfssetgoal")) {
		f = MFSSETGOAL;
	} else if (CHECKNAME("mfsrsetgoal")) {
		f = MFSSETGOAL;
		rflag = 1;
		fprintf(stderr, "deprecated tool - use \"mfssetgoal -r\"\n");
	} else if (CHECKNAME("mfsgettrashtime")) {
		f = MFSGETTRASHTIME;
	} else if (CHECKNAME("mfsrgettrashtime")) {
		f = MFSGETTRASHTIME;
		rflag = 1;
		fprintf(stderr, "deprecated tool - use \"mfsgettrashtime -r\"\n");
	} else if (CHECKNAME("mfssettrashtime")) {
		f = MFSSETTRASHTIME;
	} else if (CHECKNAME("mfsrsettrashtime")) {
		f = MFSSETTRASHTIME;
		rflag = 1;
		fprintf(stderr, "deprecated tool - use \"mfssettrashtime -r\"\n");
	} else if (CHECKNAME("mfscheckfile")) {
		f = MFSCHECKFILE;
	} else if (CHECKNAME("mfsfileinfo")) {
		f = MFSFILEINFO;
	} else if (CHECKNAME("mfsappendchunks")) {
		f = MFSAPPENDCHUNKS;
	} else if (CHECKNAME("mfsdirinfo")) {
		f = MFSDIRINFO;
	} else if (CHECKNAME("mfsgeteattr")) {
		f = MFSGETEATTR;
	} else if (CHECKNAME("mfsseteattr")) {
		f = MFSSETEATTR;
	} else if (CHECKNAME("mfsdeleattr")) {
		f = MFSDELEATTR;
	} else if (CHECKNAME("mfsfilerepair")) {
		f = MFSFILEREPAIR;
	} else if (CHECKNAME("mfsmakesnapshot")) {
		f = MFSMAKESNAPSHOT;
	} else if (CHECKNAME("mfsrepquota")) {
		f = MFSREPQUOTA;
	} else if (CHECKNAME("mfssetquota")) {
		f = MFSSETQUOTA;
	} else {
		fprintf(stderr, "unknown binary name\n");
		return 1;
	}

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

	// parse options
	switch (f) {
	case MFSMAKESNAPSHOT:
		while ((ch = getopt(argc, argv, "fo")) != -1) {
			switch (ch) {
			case 'f':
			case 'o':
				oflag = 1;
				break;
			case 'l':
				lflag = 1;
				break;
			}
		}
		argc -= optind;
		argv += optind;
		if (argc < 2) {
			usage(f);
		}
		return snapshot(argv[argc - 1], argv, argc - 1, oflag, lflag);
	case MFSGETGOAL:
	case MFSSETGOAL:
	case MFSGETTRASHTIME:
	case MFSSETTRASHTIME:
		while ((ch = getopt(argc, argv, "rnhH")) != -1) {
			switch (ch) {
			case 'n':
				humode = 0;
				break;
			case 'h':
				humode = 1;
				break;
			case 'H':
				humode = 2;
				break;
			case 'r':
				rflag = 1;
				break;
			}
		}
		argc -= optind;
		argv += optind;
		if ((f == MFSSETGOAL || f == MFSSETTRASHTIME) && argc == 0) {
			usage(f);
		}
		if (f == MFSSETGOAL) {
			goal = argv[0];
			if (!goal.empty() && (goal.back() == '-' || goal.back() == '+')) {
				fprintf(stderr, "setgoal doesn't support +/- modifiers anymore\n");
				usage(f);
			}
			argc--;
			argv++;
		}
		if (f == MFSSETTRASHTIME) {
			char *p = argv[0];
			trashtime = 0;
			while (p[0] >= '0' && p[0] <= '9') {
				trashtime *= 10;
				trashtime += (p[0] - '0');
				p++;
			}
			if (p[0] == '\0' || ((p[0] == '-' || p[0] == '+') && p[1] == '\0')) {
				if (p[0] == '-') {
					smode = SMODE_DECREASE;
				} else if (p[0] == '+') {
					smode = SMODE_INCREASE;
				}
			} else {
				fprintf(stderr,
				        "trashtime should be given as number of seconds optionally folowed by '-' "
				        "or '+'\n");
				usage(f);
			}
			argc--;
			argv++;
		}
		break;
	case MFSGETEATTR:
		while ((ch = getopt(argc, argv, "rnhH")) != -1) {
			switch (ch) {
			case 'n':
				humode = 0;
				break;
			case 'h':
				humode = 1;
				break;
			case 'H':
				humode = 2;
				break;
			case 'r':
				rflag = 1;
				break;
			}
		}
		argc -= optind;
		argv += optind;
		break;
	case MFSSETEATTR:
	case MFSDELEATTR:
		while ((ch = getopt(argc, argv, "rnhHf:")) != -1) {
			switch (ch) {
			case 'n':
				humode = 0;
				break;
			case 'h':
				humode = 1;
				break;
			case 'H':
				humode = 2;
				break;
			case 'r':
				rflag = 1;
				break;
			case 'f':
				found = 0;
				for (i = 0; found == 0 && i < EATTR_BITS; i++) {
					if (strcmp(optarg, eattrtab[i]) == 0) {
						found = 1;
						eattr |= 1 << i;
					}
				}
				if (!found) {
					fprintf(stderr, "unknown flag\n");
					usage(f);
				}
				break;
			}
		}
		argc -= optind;
		argv += optind;
		if (eattr == 0 && argc >= 1) {
			if (f == MFSSETEATTR) {
				fprintf(stderr, "no attribute(s) to set\n");
			} else {
				fprintf(stderr, "no attribute(s) to delete\n");
			}
			usage(f);
		}
		break;
	case MFSFILEREPAIR:
	case MFSDIRINFO:
	case MFSCHECKFILE:
		while ((ch = getopt(argc, argv, "nhH")) != -1) {
			switch (ch) {
			case 'n':
				humode = 0;
				break;
			case 'h':
				humode = 1;
				break;
			case 'H':
				humode = 2;
				break;
			}
		}
		argc -= optind;
		argv += optind;
		break;
	case MFSREPQUOTA:
	case MFSSETQUOTA: {
		std::vector<int> uid;
		std::vector<int> gid;
		bool reportAll = false;
		bool per_directory_quota = false;
		char *endptr = nullptr;
		std::string dir_path;
		const char *options = (f == MFSREPQUOTA) ? "nhHdu:g:a" : "du:g:";
		while ((ch = getopt(argc, argv, options)) != -1) {
			switch (ch) {
			case 'n':
				humode = 0;
				break;
			case 'h':
				humode = 1;
				break;
			case 'H':
				humode = 2;
				break;
			case 'u':
				uid.push_back(strtol(optarg, &endptr, 10));
				check_usage(f, *endptr, "invalid uid: %s\n", optarg);
				break;
			case 'g':
				gid.push_back(strtol(optarg, &endptr, 10));
				check_usage(f, *endptr, "invalid gid: %s\n", optarg);
				break;
			case 'd':
				per_directory_quota = true;
				break;
			case 'a':
				reportAll = true;
				break;
			default:
				fprintf(stderr, "invalid argument: %c", (char)ch);
				usage(f);
			}
		}
		check_usage(f, !((uid.size() + gid.size() != 0) ^ (reportAll || per_directory_quota)),
		            "provide either -a flag or uid/gid\n");
		check_usage(f, !per_directory_quota && (f == MFSSETQUOTA && uid.size() + gid.size() != 1),
		            "provide a single user/group id\n");

		argc -= optind;
		argv += optind;

		if (f == MFSSETQUOTA) {
			check_usage(f, argc != 5,
			            "expected parameters: <hard-limit-size> <soft-limit-size> "
			            "<hard-limit-inodes> <soft-limit-inodes> <mountpoint-root-path>\n");
			uint64_t quotaSoftInodes = 0, quotaHardInodes = 0, quotaSoftSize = 0, quotaHardSize = 0;
			check_usage(f, my_get_number(argv[0], &quotaSoftSize, UINT64_MAX, 1) < 0,
			            "soft-limit-size bad value\n");
			check_usage(f, my_get_number(argv[1], &quotaHardSize, UINT64_MAX, 1) < 0,
			            "hard-limit-size bad value\n");
			check_usage(f, my_get_number(argv[2], &quotaSoftInodes, UINT64_MAX, 0) < 0,
			            "soft-limit-inodes bad value\n");
			check_usage(f, my_get_number(argv[3], &quotaHardInodes, UINT64_MAX, 0) < 0,
			            "hard-limit-inodes bad value\n");

			QuotaOwner quotaOwner;
			if (!per_directory_quota) {
				sassert((uid.size() + gid.size() == 1));
				quotaOwner = ((uid.size() == 1) ? QuotaOwner(QuotaOwnerType::kUser, uid[0])
				                                : QuotaOwner(QuotaOwnerType::kGroup, gid[0]));
			} else {
				quotaOwner = QuotaOwner(QuotaOwnerType::kInode, 0);
			}

			dir_path = argv[4];
			return quota_set(dir_path, quotaOwner, quotaSoftInodes, quotaHardInodes, quotaSoftSize,
			                 quotaHardSize);
		} else {
			check_usage(f, argc != 1, "expected parameter: <mountpoint-root-path>\n");
			dir_path = argv[0];
			return quota_rep(dir_path, uid, gid, reportAll, per_directory_quota);
		}
	}
	default:
		while (getopt(argc, argv, "") != -1)
			;
		argc -= optind;
		argv += optind;
	}

	if (f == MFSAPPENDCHUNKS) {
		if (argc <= 1) {
			usage(f);
		}
		appendfname = argv[0];
		i = open(appendfname, O_RDWR | O_CREAT, 0666);
		if (i < 0) {
			fprintf(stderr, "can't create/open file: %s\n", appendfname);
			return 1;
		}
		close(i);
		argc--;
		argv++;
	}

	if (argc < 1) {
		usage(f);
	}
	status = 0;
	while (argc > 0) {
		switch (f) {
		case MFSGETGOAL:
			if (get_goal(*argv, (rflag) ? GMODE_RECURSIVE : GMODE_NORMAL) < 0) {
				status = 1;
			}
			break;
		case MFSSETGOAL:
			if (set_goal(*argv, goal, (rflag) ? (smode | SMODE_RMASK) : smode) < 0) {
				status = 1;
			}
			break;
		case MFSGETTRASHTIME:
			if (get_trashtime(*argv, (rflag) ? GMODE_RECURSIVE : GMODE_NORMAL) < 0) {
				status = 1;
			}
			break;
		case MFSSETTRASHTIME:
			if (set_trashtime(*argv, trashtime, (rflag) ? (smode | SMODE_RMASK) : smode) < 0) {
				status = 1;
			}
			break;
		case MFSCHECKFILE:
			if (check_file(*argv) < 0) {
				status = 1;
			}
			break;
		case MFSFILEINFO:
			if (file_info(*argv) < 0) {
				status = 1;
			}
			break;
		case MFSAPPENDCHUNKS:
			if (append_file(appendfname, *argv) < 0) {
				status = 1;
			}
			break;
		case MFSDIRINFO:
			if (dir_info(*argv) < 0) {
				status = 1;
			}
			break;
		case MFSFILEREPAIR:
			if (file_repair(*argv) < 0) {
				status = 1;
			}
			break;
		case MFSGETEATTR:
			if (get_eattr(*argv, (rflag) ? GMODE_RECURSIVE : GMODE_NORMAL) < 0) {
				status = 1;
			}
			break;
		case MFSSETEATTR:
			if (set_eattr(*argv, eattr, (rflag) ? (SMODE_RMASK | SMODE_INCREASE) : SMODE_INCREASE) <
			    0) {
				status = 1;
			}
			break;
		case MFSDELEATTR:
			if (set_eattr(*argv, eattr, (rflag) ? (SMODE_RMASK | SMODE_DECREASE) : SMODE_DECREASE) <
			    0) {
				status = 1;
			}
			break;
		}
		argc--;
		argv++;
	}
	return status;
}
