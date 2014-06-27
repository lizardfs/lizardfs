#include "config.h"
#include "common/metadata.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <syslog.h>
#include <unistd.h>
#include <cstring>

#include "common/datapack.h"
#include "common/mfserr.h"
#include "common/slogger.h"

#define METADATA_FILENAME_TEMPL "metadata.mfs"
const char kMetadataFilename[] = METADATA_FILENAME_TEMPL;
const char kMetadataTmpFilename[] = METADATA_FILENAME_TEMPL ".tmp";
const char kMetadataBackFilename[] = METADATA_FILENAME_TEMPL ".back";
const char kMetadataEmergencyFilename[] = METADATA_FILENAME_TEMPL ".emergency";
#undef METADATA_FILENAME_TEMPL
const char kMetadataMlBackFilename[] = "metadata_ml.mfs.back";
const char kChangelogFilename[] = "changelog.0.mfs";

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
	int fd;

	fd = open(fname.c_str(), O_RDONLY);
	if (fd < 0) {
		mfs_arg_syslog(LOG_ERR, "open failed: %s", strerr(errno));
		return 0;
	}
	fstat(fd, &st);

	size_t fileSize = st.st_size;

	const char* fileContent = (const char*) mmap(NULL, fileSize, PROT_READ, MAP_PRIVATE, fd, 0);
	if (fileContent == MAP_FAILED) {
		mfs_arg_syslog(LOG_ERR, "mmap failed: %s", strerr(errno));
		close(fd);
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
	close(fd);
	return lastLogVersion;
}

