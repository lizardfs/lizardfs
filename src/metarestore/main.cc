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

#include "common/platform.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/cfg.h"
#include "common/metadata.h"
#include "common/mfserr.h"
#include "common/rotate_files.h"
#include "common/setup.h"
#include "common/slogger.h"
#include "master/chunks.h"
#include "master/filesystem.h"
#include "master/hstring_memstorage.h"
#include "master/hstring_storage.h"
#include "master/restore.h"
#include "metarestore/merger.h"

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)

#define MAXIDHOLE 10000


int changelog_checkname(const char *fname) {
	const char *ptr = fname;
	std::string testName[] = {
		kChangelogFilename,
		kChangelogMlFilename
	};

	for (const std::string& s : testName) {
		if (strncmp(ptr, s.c_str(), s.length()) == 0) {
			ptr += s.length();
			if (*ptr == '.') {
				++ptr;
				while (isdigit(*ptr)) {
					++ptr;
				}
				if (*ptr == 0) {
					return 1;
				}
			} else if (*ptr == 0) {
				return 1;
			}
		}
	}

	std::string testNameOld[] = {
		"changelog.",
		"changelog_ml.",
		"changelog_ml_back."
	};

	for (const std::string& s : testNameOld) {
		if (strncmp(ptr, s.c_str(), s.length()) == 0) {
			ptr += s.length();
			if (isdigit(*ptr)) {
				while (isdigit(*ptr)) {
					++ ptr;
				}
				if (strcmp(ptr, ".mfs") == 0) {
					return 1;
				}
			}
		}
	}
	return 0;
}

void usage(const char* appname) {
	lzfs_pretty_syslog(LOG_ERR, "invalid/missing arguments");
	fprintf(stderr, "restore metadata:\n"
			"\t%s [-c] [-k <checksum>] [-z] [-f] [-b] [-i] [-x [-x]] [-B n] -m <meta data file> -o "
			"<restored meta data file> [ <change log file> [ <change log file> [ .... ]]\n"
			"dump metadata:\n"
			"\t%s [-i] -m <meta data file>\n"
			"autorestore:\n"
			"\t%s [-f] [-z] [-b] [-i] [-x [-x]] [-B n] -a [-d <data path>]\n"
			"print version of metadata that can be read from disk by a master server in auto recovery mode:\n"
			"\t%s -g -d <data path>\n"
			"print version:\n"
			"\t%s -v\n"
			"\n"
			"-B n - keep n backup copies of metadata file\n"
			"-c   - print checksum of the metadata\n"
			"-k   - check checksum against given checksum\n"
			"-z   - ignore metadata checksum inconsistency while applying changelogs\n"
			"-x   - produce more verbose output\n"
			"-xx  - even more verbose output\n"
			"-b   - if there is any error in change logs then save the best possible metadata file\n"
			"-i   - ignore some metadata structure errors (attach orphans to root, ignore names without inode, etc.)\n"
			"-f   - force loading all changelogs\n", appname, appname, appname, appname, appname);
}

/*! \brief Prints version of metadata that can be read from disk
 * by a master server in auto recovery mode.
 *
 * \param path - path to directory containing metadata files.
 */
void meta_version_on_disk(std::string path) {
	static const std::string changelogs[] {
		std::string(kChangelogFilename) + ".2",
		std::string(kChangelogFilename) + ".1",
		kChangelogFilename
	};
	uint64_t metadata_version;

	try {
		metadata_version = metadataGetVersion(path + "/" + kMetadataFilename);
		// check if it is a new installation
		if (metadata_version == 0) {
			printf("1\n");
			return;
		}
	} catch (MetadataCheckException& ex) {
		lzfs_pretty_syslog(LOG_WARNING, "malformed metadata file: %s", ex.what());
		printf("0\n");
		return;
	}



	bool oldExists = false;
	for (const std::string& s : changelogs) {
		std::string fullFileName = path + "/" + s;
		try {
			if (fs::exists(fullFileName)) {
				oldExists = true;
				uint64_t first = changelogGetFirstLogVersion(fullFileName);
				uint64_t last = changelogGetLastLogVersion(fullFileName);
				if (last >= first  && first <= metadata_version) {
					if (last >= metadata_version) {
						metadata_version = last + 1;
					}
				} else {
					printf("0\n");
					return;
				}
			} else if (oldExists && fullFileName != kChangelogFilename) {
				lzfs_pretty_syslog(LOG_WARNING, "changelog `%s' missing", fullFileName.c_str());
			}
		} catch (FilesystemException& ex) {
			lzfs_pretty_syslog(LOG_WARNING, "exception while fs:exists: %s", ex.what());
			printf("0\n");
			return;
		}
	}

	printf("%" PRIu64 "\n", metadata_version);
}

int main(int argc,char **argv) {
	int ch;
	uint8_t vl=0;
	bool autorestore = false;
	bool versionRecovery = false;
	int savebest = 0;
	int ignoreflag = 0;
	int forcealllogs = 0;
	bool printhash = false;
	int skip;
	std::string metaout, metadata, datapath;
	char *appname = argv[0];
	uint64_t firstlv,lastlv;
	std::unique_ptr<uint64_t> expectedChecksum;
	int storedPreviousBackMetaCopies = kMaxStoredPreviousBackMetaCopies;
	bool noLock = false;

	hstorage::Storage::reset(new hstorage::MemStorage());

	prepareEnvironment();
	openlog(nullptr, LOG_PID | LOG_NDELAY, LOG_USER);

	while ((ch = getopt(argc, argv, "gfck:vm:o:d:abB:xih:z#?")) != -1) {
		switch (ch) {
			case 'g':
				versionRecovery = true;
				break;
			case 'v':
				printf("version: %s\n", LIZARDFS_PACKAGE_VERSION);
				return 0;
			case 'o':
				metaout = optarg;
				break;
			case 'm':
				metadata = optarg;
				break;
			case 'd':
				datapath = optarg;
				if (datapath.length() > 1) {
					if (datapath.back() == '/') {
						datapath.resize(datapath.length() - 1);
					}
				}
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
			case 'B':
				storedPreviousBackMetaCopies = atoi(optarg);
				break;
			case 'i':
				ignoreflag=1;
				break;
			case 'f':
				forcealllogs=1;
				break;
			case 'c':
				printhash = true;
				break;
			case 'k':
				expectedChecksum.reset(new uint64_t);
				char* endPtr;
				*expectedChecksum = strtoull(optarg, &endPtr, 10);
				if (*endPtr != '\0') {
					lzfs_pretty_syslog(LOG_ERR, "invalid checksum: %s", optarg);
					return 1;
				}
				break;
			case 'z':
				fs_disable_checksum_verification(true);
				break;
			case '#':
				noLock = true;
				break;
			case '?':
			default:
				usage(argv[0]);
				return 1;
		}
	}
	argc -= optind;
	argv += optind;

	// bad usage of -m
	if (versionRecovery && datapath.empty()) {
		usage(appname);
		return 1;
	}

	// bad usage of -a
	if (autorestore && (!metadata.empty() || !metaout.empty())) {
		usage(appname);
		return 1;
	}

	// only datapath passed (also bad)
	if (!autorestore && !versionRecovery && (metadata.empty() || !datapath.empty())) {
		usage(appname);
		return 1;
	}

	restore_setverblevel(vl);

	if (versionRecovery) {
		meta_version_on_disk(datapath);
		return 0;
	}

	if (autorestore) {
		if (datapath.empty()) {
			datapath = DATA_PATH;
		}
		// All candidates from the least to the most preferred one
		// We still load metadata.mfs.back for scenarios where metaretore
		// is started during migration to lizardfs version with new naming scheme.
		std::string candidates[] {
			std::string(kMetadataMlFilename) + ".back.1",
			std::string(kMetadataFilename) + ".back.1",
			std::string(kMetadataMlFilename) + ".1",
			std::string(kMetadataFilename) + ".1",
			kMetadataMlFilename,
			kMetadataFilename};
		std::string bestmetadata;
		uint64_t bestversion = 0;
		for (auto candidate : candidates) {
			std::string metadata_candidate = std::string(datapath) + "/" + candidate;
			if (access(metadata_candidate.c_str(), F_OK) != 0) {
				continue;
			}
			try {
				uint64_t version = metadataGetVersion(metadata_candidate.c_str());
				if (version >= bestversion) {
					bestversion = version;
					bestmetadata = metadata_candidate;
				}
			} catch (MetadataCheckException& ex) {
				lzfs_pretty_syslog(LOG_NOTICE, "skipping malformed metadata file %s: %s", candidate.c_str(), ex.what());
			}
		}
		if (bestmetadata.empty()) {
			lzfs_pretty_syslog(LOG_ERR, "can't find backed up metadata file");
			return 1;
		}
		metadata = bestmetadata;
		metaout =  datapath + "/" + kMetadataFilename;
		lzfs_pretty_syslog(LOG_INFO, "file %s will be used to restore the most recent metadata",
				metadata.c_str());
	}
	try {
		if (fs_init(metadata.c_str(), ignoreflag, noLock) != 0) {
			lzfs_pretty_syslog(LOG_NOTICE, "error: can't read metadata from file: %s", metadata.c_str());
			return 1;
		}
	} catch (const std::exception& e) {
		lzfs_pretty_syslog(LOG_ERR, "error: can't read metadata from file: %s, %s", metadata.c_str(), e.what());
		return 1;
	}
	if (fs_getversion() == 0) {
		lzfs_pretty_syslog(LOG_ERR, "invalid metadata version (0)");
		return 1;
	}
	if (vl > 0) {
		lzfs_pretty_syslog(LOG_NOTICE, "loaded metadata with version %" PRIu64 "", fs_getversion());
	}

	if (autorestore) {
		std::vector<std::string> filenames;
		DIR *dd = opendir(datapath.c_str());
		if (!dd) {
			lzfs_pretty_syslog(LOG_ERR, "can't open data directory");
			return 1;
		}
		rewinddir(dd);
		struct dirent *dp;
		while ((dp = readdir(dd)) != NULL) {
			if (changelog_checkname(dp->d_name)) {
				filenames.push_back(datapath + "/" + dp->d_name);
				firstlv = changelogGetFirstLogVersion(filenames.back());
				try {
					lastlv = changelogGetLastLogVersion(filenames.back());
				} catch (const Exception& ex) {
					lzfs_pretty_syslog(LOG_WARNING, "%s", ex.what());
					lastlv = 0;
				}
				skip = ((lastlv<fs_getversion() || firstlv==0) && forcealllogs==0)?1:0;
				if (vl>0) {
					std::ostringstream oss;
					if (skip) {
						oss << "skipping changelog file: ";
					} else {
						oss << "using changelog file: ";
					}
					oss << filenames.back() << " (changes: ";
					if (firstlv > 0) {
						oss << firstlv;
					} else {
						oss << "???";
					}
					oss << " - ";
					if (lastlv > 0) {
						oss << lastlv;
					} else {
						oss << "???";
					}
					oss << ")";
					lzfs_pretty_syslog(LOG_NOTICE, "%s", oss.str().c_str());
				}
				if (skip) {
					filenames.pop_back();
				}
			}
		}
		closedir(dd);
		if (filenames.empty() && metadata == metaout) {
			lzfs_pretty_syslog(LOG_NOTICE, "nothing to do, exiting without changing anything");
			if (!noLock) {
				fs_unlock();
			}
			return 0;
		}
		merger_start(filenames, MAXIDHOLE);
	} else {
		uint32_t pos;
		std::vector<std::string> filenames;

		for (pos=0 ; (int32_t)pos<argc ; pos++) {
			firstlv = changelogGetFirstLogVersion(argv[pos]);
			try {
				lastlv = changelogGetLastLogVersion(argv[pos]);
			} catch (const Exception& ex) {
				lzfs_pretty_syslog(LOG_WARNING, "%s", ex.what());
				lastlv = 0;
			}
			skip = ((lastlv<fs_getversion() || firstlv==0) && forcealllogs==0)?1:0;
			if (vl>0) {
				std::ostringstream oss;
				if (skip) {
					oss << "skipping changelog file: ";
				} else {
					oss << "using changelog file: ";
				}
				oss << argv[pos] << " (changes: ";
				if (firstlv > 0) {
					oss << firstlv;
				} else {
					oss << "???";
				}
				oss << " - ";
				if (lastlv > 0) {
					oss << lastlv;
				} else {
					oss << "???";
				}
				oss << ")";
				lzfs_pretty_syslog(LOG_NOTICE, "%s", oss.str().c_str());
			}
			if (skip==0) {
				filenames.push_back(argv[pos]);
			}
		}
		merger_start(filenames, MAXIDHOLE);
	}

	uint8_t status = merger_loop();

	if (status != LIZARDFS_STATUS_OK && savebest==0) {
		return 1;
	}

	int returnStatus = 0;
	uint64_t checksum = fs_checksum(ChecksumMode::kForceRecalculate);
	if (printhash) {
		printf("%" PRIu64 "\n", checksum);
	}
	if (expectedChecksum) {
		returnStatus = *expectedChecksum == checksum ? 0 : 2;
		printf("%s\n", returnStatus ? "ERR" : "OK");
	}
	if (metaout.empty()) {
		fs_dump();
		chunk_dump();
	} else {
		lzfs_pretty_syslog(LOG_NOTICE, "store metadata into file: %s", metaout.c_str());
		if (metaout == metadata) {
			rotateFiles(metaout, storedPreviousBackMetaCopies);
		}
		fs_term(metaout.c_str(), noLock);
	}
	return returnStatus;
}
