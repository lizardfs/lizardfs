#include "config.h"
#include "common/metadata.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <syslog.h>
#include <unistd.h>
#include <cstring>

#include "common/datapack.h"

uint64_t metadata_getversion(const std::string& file) {
	int fd;
	char chkbuff[16];
	char eofmark[16];

	fd = open(file.c_str(), O_RDONLY);
	if (fd<0) {
		throw MetadataCheckException("Can't open the metadata file");
	}
	if (read(fd,chkbuff,16)!=16) {
		close(fd);
		throw MetadataCheckException("Can't read the metadata file");
	}
	if (memcmp(chkbuff,"MFSM NEW",8)==0) {
		close(fd);
		return 0;
	}
	if (memcmp(chkbuff,MFSSIGNATURE "M 1.5",8)==0 || memcmp(chkbuff,MFSSIGNATURE "M 1.6",8)==0) {
		memset(eofmark,0,16);
	} else if (memcmp(chkbuff,MFSSIGNATURE "M 2.0",8)==0) {
		memcpy(eofmark,"[MFS EOF MARKER]",16);
	} else {
		close(fd);
		throw MetadataCheckException("Bad format of the metadata file");
	}
	const uint8_t* ptr = reinterpret_cast<const uint8_t*>(chkbuff + 8);
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
