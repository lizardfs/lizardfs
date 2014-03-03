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

#include "config.h"

#include <errno.h>
#include <fcntl.h>
#include <dirent.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string>
#include <vector>

#include "common/metadata.h"
#include "common/slogger.h"
#include "common/strerr.h"
#include "master/filesystem.h"
#include "master/chunks.h"
#include "metarestore/merger.h"
#include "metarestore/restore.h"

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)
const char id[]="@(#) version: " STR(PACKAGE_VERSION_MAJOR) "." STR(PACKAGE_VERSION_MINOR) "." STR(PACKAGE_VERSION_MICRO) ", written by Jakub Kruszona-Zawadzki";

#define MAXIDHOLE 10000

uint64_t findfirstlogversion(const char *fname) {
	uint8_t buff[50];
	int32_t s,p;
	uint64_t fv;
	int fd;

	fd = open(fname,O_RDONLY);
	if (fd<0) {
		return 0;
	}
	s = read(fd,buff,50);
	close(fd);
	if (s<=0) {
		return 0;
	}
	fv = 0;
	p = 0;
	while (p<s && buff[p]>='0' && buff[p]<='9') {
		fv *= 10;
		fv += buff[p]-'0';
		p++;
	}
	if (p>=s || buff[p]!=':') {
		return 0;
	}
	return fv;
}

uint64_t findlastlogversion(const char *fname) {
	struct stat st;
	int fd;

	fd = open(fname, O_RDONLY);
	if (fd < 0) {
		fprintf(stderr, "open failed: %s\n", strerr(errno));
		return 0;
	}
	fstat(fd, &st);

	size_t fileSize = st.st_size;

	const char* fileContent = (const char*) mmap(NULL, fileSize, PROT_READ, MAP_PRIVATE, fd, 0);
	if (fileContent == MAP_FAILED) {
		fprintf(stderr, "mmap failed: %s\n", strerr(errno));
		close(fd);
		return 0; // 0 counterintuitively means failure
	}
	uint64_t lastLogVersion = 0;
	// first LF is (should be) the last byte of the file
	if (fileSize == 0 || fileContent[fileSize - 1] != '\n') {
		fprintf(stderr, "truncated changelog (%s) (no LF at the end of the last line)\n", fname);
	} else {
		size_t pos = fileSize - 1;
		while (pos > 0) {
			--pos;
			if (fileContent[pos] == '\n') {
				break;
			}
		}
		char *endPtr = NULL;
		lastLogVersion = strtoull(fileContent + pos, &endPtr, 10);
		if (*endPtr != ':') {
			fprintf(stderr, "malformed changelog (%s) (expected colon after change number)\n",
					fname);
			lastLogVersion = 0;
		}
	}
	if (munmap((void*) fileContent, fileSize)) {
		fprintf(stderr, "munmap failed: %s\n", strerr(errno));
	}
	close(fd);
	return lastLogVersion;
}

int changelog_checkname(const char *fname) {
	const char *ptr = fname;
	if (strncmp(ptr,"changelog.",10)==0) {
		ptr+=10;
		if (*ptr>='0' && *ptr<='9') {
			while (*ptr>='0' && *ptr<='9') {
				ptr++;
			}
			if (strcmp(ptr,".mfs")==0) {
				return 1;
			}
		}
	} else if (strncmp(ptr,"changelog_ml.",13)==0) {
		ptr+=13;
		if (*ptr>='0' && *ptr<='9') {
			while (*ptr>='0' && *ptr<='9') {
				ptr++;
			}
			if (strcmp(ptr,".mfs")==0) {
				return 1;
			}
		}
	} else if (strncmp(ptr,"changelog_ml_back.",18)==0) {
		ptr+=18;
		if (*ptr>='0' && *ptr<='9') {
			while (*ptr>='0' && *ptr<='9') {
				ptr++;
			}
			if (strcmp(ptr,".mfs")==0) {
				return 1;
			}
		}
	}
	return 0;
}

void usage(const char* appname) {
	fprintf(stderr,"restore metadata:\n\t%s [-f] [-b] [-i] [-x [-x]] -m <meta data file> -o <restored meta data file> [ <change log file> [ <change log file> [ .... ]]\ndump metadata:\n\t%s [-i] -m <meta data file>\nautorestore:\n\t%s [-f] [-b] [-i] [-x [-x]] -a [-d <data path>]\nprint version:\n\t%s -v\n\n-x - produce more verbose output\n-xx - even more verbose output\n-b - if there is any error in change logs then save the best possible metadata file\n-i - ignore some metadata structure errors (attach orphans to root, ignore names without inode, etc.)\n-f - force loading all changelogs\n",appname,appname,appname,appname);
}

int main(int argc,char **argv) {
	int ch;
	uint8_t vl=0;
	bool autorestore = false;
	int savebest = 0;
	int ignoreflag = 0;
	int forcealllogs = 0;
	int status;
	int skip;
	std::string metaout, metadata, datapath;
	char *appname = argv[0];
	uint64_t firstlv,lastlv;

	strerr_init();

	while ((ch = getopt(argc, argv, "fvm:o:d:abxih:?")) != -1) {
		switch (ch) {
			case 'v':
				printf("version: %u.%u.%u\n",PACKAGE_VERSION_MAJOR,PACKAGE_VERSION_MINOR,PACKAGE_VERSION_MICRO);
				return 0;
			case 'o':
				metaout = optarg;
				break;
			case 'm':
				metadata = optarg;
				break;
			case 'd':
				datapath = optarg;
				break;
			case 'x':
				vl++;
				break;
			case 'a':
				autorestore = true;
				break;
			case 'b':
				savebest=1;
				break;
			case 'i':
				ignoreflag=1;
				break;
			case 'f':
				forcealllogs=1;
				break;
			case '?':
			default:
				usage(argv[0]);
				return 1;
		}
	}
	argc -= optind;
	argv += optind;

	if ((!autorestore && (metadata.empty() || !datapath.empty()))
			|| (autorestore && (!metadata.empty() || !metaout.empty()))) {
		usage(appname);
		return 1;
	}

	restore_setverblevel(vl);

	if (autorestore) {
		if (datapath.empty()) {
			datapath = DATA_PATH;
		}
		// All candidates from the least to the most preferred one
		auto candidates{
			"metadata_ml.mfs.back.1",
			"metadata.mfs.back.1",
			"metadata_ml.mfs.back",
			"metadata.mfs.back",
			"metadata.mfs"};
		std::string bestmetadata;
		uint64_t bestversion = 0;
		for (const char* candidate : candidates) {
			std::string metadata_candidate = std::string(datapath) + "/" + candidate;
			if (access(metadata_candidate.c_str(), F_OK) != 0) {
				continue;
			}
			try {
				uint64_t version = metadata_getversion(metadata_candidate.c_str());
				if (version >= bestversion) {
					bestversion = version;
					bestmetadata = metadata_candidate;
				}
			} catch (MetadataCheckException& ex) {
				printf("skipping malformed metadata file %s: %s\n", candidate, ex.what());
			}
		}
		if (bestmetadata.empty()) {
			printf("error: can't find backed up metadata file !!!\n");
			return 1;
		}
		metadata = bestmetadata;
		metaout =  datapath + "/metadata.mfs";
		printf("file %s will be used to restore the most recent metadata\n", metadata.c_str());
	}

	if (fs_init(metadata.c_str(), ignoreflag) != 0) {
		printf("error: can't read metadata from file: %s\n", metadata.c_str());
		return 1;
	}
	if (fs_getversion() == 0) {
		// TODO(msulikowski) make it work! :)
		printf("error: applying changes to an empty metadata file (version 0) not supported!!!\n");
		return 1;
	}
	if (vl > 0) {
		printf("loaded metadata with version %" PRIu64 "\n", fs_getversion());
	}

	if (autorestore) {
		std::vector<char*> filenames;
		DIR *dd = opendir(datapath.c_str());
		if (!dd) {
			printf("can't open data directory\n");
			return 1;
		}
		rewinddir(dd);
		struct dirent *dp;
		while ((dp = readdir(dd)) != NULL) {
			if (changelog_checkname(dp->d_name)) {
				filenames.push_back(strdup((datapath + "/" + dp->d_name).c_str()));
				firstlv = findfirstlogversion(filenames.back());
				lastlv = findlastlogversion(filenames.back());
				skip = ((lastlv<fs_getversion() || firstlv==0) && forcealllogs==0)?1:0;
				if (vl>0) {
					if (skip) {
						printf("skipping changelog file: %s (changes: ", filenames.back());
					} else {
						printf("using changelog file: %s (changes: ", filenames.back());
					}
					if (firstlv>0) {
						printf("%" PRIu64 " - ",firstlv);
					} else {
						printf("??? - ");
					}
					if (lastlv>0) {
						printf("%" PRIu64 ")\n",lastlv);
					} else {
						printf("?\?\?)\n");
					}
				}
				if (skip) {
					filenames.pop_back();
				}
			}
		}
		closedir(dd);
		if (filenames.empty() && metadata == metaout) {
			printf("nothing to do, exiting without changing anything\n");
			return 0;
		}
		merger_start(filenames.size(), filenames.data(), MAXIDHOLE);
		for (uint32_t pos = 0 ; pos < filenames.size() ; pos++) {
			free(filenames[pos]);
		}
	} else {
		uint32_t files,pos;
		char **filenames;

		filenames = (char**)malloc(sizeof(char*)*argc);
		files = 0;
		for (pos=0 ; (int32_t)pos<argc ; pos++) {
			firstlv = findfirstlogversion(argv[pos]);
			lastlv = findlastlogversion(argv[pos]);
			skip = ((lastlv<fs_getversion() || firstlv==0) && forcealllogs==0)?1:0;
			if (vl>0) {
				if (skip) {
					printf("skipping changelog file: %s (changes: ",argv[pos]);
				} else {
					printf("using changelog file: %s (changes: ",argv[pos]);
				}
				if (firstlv>0) {
					printf("%" PRIu64 " - ",firstlv);
				} else {
					printf("??? - ");
				}
				if (lastlv>0) {
					printf("%" PRIu64 ")\n",lastlv);
				} else {
					printf("?\?\?)\n");
				}
			}
			if (skip==0) {
				filenames[files] = strdup(argv[pos]);
				files++;
			}
		}
		merger_start(files,filenames,MAXIDHOLE);
		for (pos = 0 ; pos<files ; pos++) {
			free(filenames[pos]);
		}
		free(filenames);
	}

	status = merger_loop();

	if (status<0 && savebest==0) {
		return 1;
	}

	if (metaout.empty()) {
		fs_dump();
		chunk_dump();
	} else {
		printf("store metadata into file: %s\n",metaout.c_str());
		fs_term(metaout.c_str());
	}
	return 0;
}
