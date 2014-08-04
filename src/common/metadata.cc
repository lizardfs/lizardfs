#include "common/platform.h"
#include "common/metadata.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <syslog.h>
#include <unistd.h>
#include <cstring>
#include <boost/filesystem.hpp>

#include "common/cwrap.h"
#include "common/datapack.h"
#include "common/mfserr.h"
#include "common/slogger.h"

#define METADATA_FILENAME_TEMPL "metadata.mfs"
const char kMetadataFilename[] = METADATA_FILENAME_TEMPL;
const char kMetadataTmpFilename[] = METADATA_FILENAME_TEMPL ".tmp";
const char kMetadataEmergencyFilename[] = METADATA_FILENAME_TEMPL ".emergency";
#undef METADATA_FILENAME_TEMPL
#define METADATA_ML_FILENAME_TEMPL "metadata_ml.mfs"
const char kMetadataMlFilename[] = METADATA_ML_FILENAME_TEMPL;
const char kMetadataMlTmpFilename[] = METADATA_ML_FILENAME_TEMPL ".tmp";
#undef METADATA_ML_FILENAME_TEMPL
#define CHANGELOG_FILENAME "changelog.mfs"
const char kChangelogFilename[] = CHANGELOG_FILENAME;
const char kChangelogTmpFilename[] = CHANGELOG_FILENAME ".tmp";
#undef CHANGELOG_FILENAME
#define CHANGELOG_ML_FILENAME "changelog_ml.mfs"
const char kChangelogMlFilename[] = CHANGELOG_ML_FILENAME;
const char kChangelogMlTmpFilename[] = CHANGELOG_ML_FILENAME ".tmp";
#undef CHANGELOG_ML_FILENAME
#define SESSIONS_ML_FILENAME "sessions_ml.mfs"
const char kSessionsMlFilename[] = SESSIONS_ML_FILENAME;
const char kSessionsMlTmpFilename[] = SESSIONS_ML_FILENAME ".tmp";
#undef SESSIONS_ML_FILENAME
#define SESSIONS_FILENAME "sessions.mfs"
const char kSessionsFilename[] = SESSIONS_FILENAME;
const char kSessionsTmpFilename[] = SESSIONS_FILENAME ".tmp";
#undef SESSIONS_FILENAME

std::unique_ptr<Lockfile> gMetadataLockfile;

uint64_t metadataGetVersion(const std::string& file) {
	int fd;
	char chkbuff[20];
	char eofmark[16];

	fd = open(file.c_str(), O_RDONLY);
	if (fd<0) {
		throw MetadataCheckException("Can't open the metadata file");
	}
	if (read(fd,chkbuff,20)!=20) {
		close(fd);
		throw MetadataCheckException("Can't read the metadata file");
	}
	if (memcmp(chkbuff,"MFSM NEW",8)==0) {
		close(fd);
		return 0;
	}
	if (memcmp(chkbuff,MFSSIGNATURE "M 1.",7)==0 && chkbuff[7]>='5' && chkbuff[7]<='6') {
		memset(eofmark,0,16);
	} else if (memcmp(chkbuff,MFSSIGNATURE "M 2.0",8)==0) {
		memcpy(eofmark,"[MFS EOF MARKER]",16);
	} else {
		close(fd);
		throw MetadataCheckException("Bad format of the metadata file");
	}
	const uint8_t* ptr = reinterpret_cast<const uint8_t*>(chkbuff + 8 + 4);
	uint64_t version;
	version = get64bit(&ptr);
	lseek(fd,-16,SEEK_END);
	if (read(fd,chkbuff,16)!=16) {
		close(fd);
		throw MetadataCheckException("Can't read the metadata file");
	}
	if (memcmp(chkbuff,eofmark,16)!=0) {
		close(fd);
		throw MetadataCheckException("The metadata file is truncated");
	}
	return version;
}

uint64_t changelogGetFirstLogVersion(const std::string& fname) {
	uint8_t buff[50];
	int32_t s,p;
	uint64_t fv;
	int fd;

	fd = open(fname.c_str(), O_RDONLY);
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

uint64_t changelogGetLastLogVersion(const std::string& fname) {
	struct stat st;

	FileDescriptor fd(open(fname.c_str(), O_RDONLY));
	if (fd.get() < 0) {
		mfs_arg_syslog(LOG_ERR, "open failed: %s", strerr(errno));
		return 0;
	}
	fstat(fd.get(), &st);

	size_t fileSize = st.st_size;
	if (fileSize == 0) {
		return 0;
	}

	const char* fileContent = (const char*) mmap(NULL, fileSize, PROT_READ, MAP_PRIVATE, fd.get(), 0);
	if (fileContent == MAP_FAILED) {
		mfs_arg_syslog(LOG_ERR, "mmap failed: %s", strerr(errno));
		return 0; // 0 counterintuitively means failure
	}
	uint64_t lastLogVersion = 0;
	// first LF is (should be) the last byte of the file
	if (fileSize == 0 || fileContent[fileSize - 1] != '\n') {
		mfs_arg_syslog(LOG_ERR, "truncated changelog (%s) (no LF at the end of the last line)",
				fname.c_str());
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
			mfs_arg_syslog(LOG_ERR, "malformed changelog (%s) (expected colon after change number)",
					fname.c_str());
			lastLogVersion = 0;
		}
	}
	if (munmap((void*) fileContent, fileSize)) {
		mfs_arg_syslog(LOG_ERR, "munmap failed: %s", strerr(errno));
	}
	return lastLogVersion;
}

void changelogsMigrateFrom_1_6_29(const std::string& fname) {
	std::string name_new, name_old;
	namespace fs = boost::filesystem;
	for (uint32_t i = 0; i < 99; i++) {
	// 99 is the maximum number of changelog file in versions up to 1.6.29.
		name_old = fname + "." + std::to_string(i) + ".mfs";
		name_new = fname + ".mfs";
		if (i != 0) {
			name_new += "." + std::to_string(i);
		}
		try {
			if (fs::exists(name_old)) {
				if (!fs::exists(name_new)) {
					fs::rename(name_old, name_new);
				} else {
					syslog(LOG_WARNING, "both old and new changelog files exist - %s and %s",
							name_old.c_str(), name_new.c_str());
				}
			}
		} catch (fs::filesystem_error& ex) {
			throw FilesystemException(
					"Error when migrating changelogs from version 1.6.29" + std::string(ex.what()));
		}
	}
}
