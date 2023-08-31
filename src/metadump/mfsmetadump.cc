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
#include "common/serialization.h"

#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <vector>

#include "common/datapack.h"
#include "protocol/MFSCommunication.h"

#include <master/filesystem_node.h>

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)

#define MAX_INDEX 0x7FFFFFFF
#define MAX_CHUNKS_PER_FILE (MAX_INDEX+1)

namespace dump {

static inline char dispchar(uint8_t c) {
	return (c>=32 && c<=126)?c:'.';
}

int chunk_load(FILE *fd, bool loadLockIds) {
	uint8_t hdr[8];
	const uint8_t *ptr;
	int32_t r;
	uint64_t chunkid,nextchunkid;
	uint32_t version,lockedto,lockid;

	if (fread(hdr,1,8,fd)!=8) {
		return -1;
	}
	ptr = hdr;
	nextchunkid = get64bit(&ptr);
	printf("# nextchunkid: %016" PRIX64 "\n",nextchunkid);
	uint32_t serializedChunkSize = (loadLockIds ? 20 : 16);
	std::vector<uint8_t> loadbuff(serializedChunkSize);
	for (;;) {
		r = fread(loadbuff.data(), 1, serializedChunkSize, fd);
		(void)r;
		ptr = loadbuff.data();
		chunkid = get64bit(&ptr);
		version = get32bit(&ptr);
		lockedto = get32bit(&ptr);
		if (loadLockIds) {
			lockid = get32bit(&ptr);
		} else {
			lockid = 1;
		}
		if (chunkid==0 && version==0 && lockedto==0) {
			return 0;
		}
		printf("*|i:%016" PRIX64 "|v:%08" PRIX32 "|t:%10" PRIu32 "|l:%10" PRIu32 "\n",chunkid,version,lockedto,lockid);
	}
	return -1;
}

void print_name(FILE *in,uint32_t nleng) {
	uint8_t buff[1024];
	uint32_t x,y,i;
	size_t happy;
	while (nleng>0) {
		y = (nleng>1024)?1024:nleng;
		x = fread(buff,1,y,in);
		for (i=0 ; i<x ; i++) {
			if (buff[i]<32 || buff[i]>127) {
				buff[i]='.';
			}
		}
		happy = fwrite(buff,1,x,stdout);
		(void)happy;
		if (x!=y) {
			return;
		}
		nleng -= x;
	}
}

int fs_loadedge(FILE *fd) {
	uint8_t uedgebuff[4+4+2];
	const uint8_t *ptr;
	uint32_t parent_id;
	uint32_t child_id;
	uint16_t nleng;

	if (fread(uedgebuff,1,4+4+2,fd)!=4+4+2) {
		fprintf(stderr,"loading edge: read error\n");
		return -1;
	}
	ptr = uedgebuff;
	parent_id = get32bit(&ptr);
	child_id = get32bit(&ptr);
	if (parent_id==0 && child_id==0) {      // last edge
		return 1;
	}
	nleng = get16bit(&ptr);

	if (parent_id==0) {
		printf("E|p:      NULL|c:%10" PRIu32 "|n:",child_id);
	} else {
		printf("E|p:%10" PRIu32 "|c:%10" PRIu32 "|n:", parent_id, child_id);
	}
	print_name(fd,nleng);
	printf("\n");
	return 0;
}

int fs_loadnode(FILE *fd) {
	uint8_t unodebuff[4+1+2+4+4+4+4+4+4+8+4+2+8*65536+4*65536+4];
	const uint8_t *ptr,*chptr;
	uint8_t type,goal;
	uint32_t nodeid,uid,gid,atimestamp,mtimestamp,ctimestamp,trashtime;
	uint16_t mode;
	char c;

	type = fgetc(fd);
	if (type==0) {  // last node
		return 1;
	}
	switch (type) {
	case TYPE_DIRECTORY:
	case TYPE_FIFO:
	case TYPE_SOCKET:
		if (fread(unodebuff,1,4+1+2+4+4+4+4+4+4,fd)!=4+1+2+4+4+4+4+4+4) {
			fprintf(stderr,"loading node: read error\n");
			return -1;
		}
		break;
	case TYPE_BLOCKDEV:
	case TYPE_CHARDEV:
	case TYPE_SYMLINK:
		if (fread(unodebuff,1,4+1+2+4+4+4+4+4+4+4,fd)!=4+1+2+4+4+4+4+4+4+4) {
			fprintf(stderr,"loading node: read error\n");
			return -1;
		}
		break;
	case TYPE_FILE:
	case TYPE_TRASH:
	case TYPE_RESERVED:
		if (fread(unodebuff,1,4+1+2+4+4+4+4+4+4+8+4+2,fd)!=4+1+2+4+4+4+4+4+4+8+4+2) {
			fprintf(stderr,"loading node: read error\n");
			return -1;
		}
		break;
	default:
		fprintf(stderr,"loading node: unrecognized node type: %c\n",type);
		return -1;
	}
	c='?';
	switch (type) {
	case TYPE_DIRECTORY:
		c='D';
		break;
	case TYPE_SOCKET:
		c='S';
		break;
	case TYPE_FIFO:
		c='F';
		break;
	case TYPE_BLOCKDEV:
		c='B';
		break;
	case TYPE_CHARDEV:
		c='C';
		break;
	case TYPE_SYMLINK:
		c='L';
		break;
	case TYPE_FILE:
		c='-';
		break;
	case TYPE_TRASH:
		c='T';
		break;
	case TYPE_RESERVED:
		c='R';
		break;
	}
	ptr = unodebuff;
	nodeid = get32bit(&ptr);
	goal = get8bit(&ptr);
	mode = get16bit(&ptr);
	uid = get32bit(&ptr);
	gid = get32bit(&ptr);
	atimestamp = get32bit(&ptr);
	mtimestamp = get32bit(&ptr);
	ctimestamp = get32bit(&ptr);
	trashtime = get32bit(&ptr);

	printf("%c|i:%10" PRIu32 "|#:%" PRIu8 "|e:%1" PRIX16 "|m:%04" PRIo16
	       "|u:%10" PRIu32 "|g:%10" PRIu32 "|a:%10" PRIu32 ",m:%10" PRIu32
	       ",c:%10" PRIu32 "|t:%10" PRIu32,
	       c,nodeid,goal,(uint16_t)(mode>>12),(uint16_t)(mode&0xFFF),
	       uid,gid,atimestamp,mtimestamp,ctimestamp,trashtime);

	if (type==TYPE_BLOCKDEV || type==TYPE_CHARDEV) {
		uint32_t rdev;
		rdev = get32bit(&ptr);
		printf("|d:%5" PRIu32 ",%5" PRIu32 "\n",rdev>>16,rdev&0xFFFF);
	} else if (type==TYPE_SYMLINK) {
		uint32_t pleng;
		pleng = get32bit(&ptr);
		printf("|p:");
		print_name(fd,pleng);
		printf("\n");
	} else if (type==TYPE_FILE || type==TYPE_TRASH || type==TYPE_RESERVED) {
		uint64_t length,chunkid;
		uint32_t ci,ch,sessionid;
		uint16_t sessionids;

		length = get64bit(&ptr);
		ch = get32bit(&ptr);
		sessionids = get16bit(&ptr);

		printf("|l:%20" PRIu64 "|c:(",length);
		while (ch>65536) {
			chptr = ptr;
			if (fread((uint8_t*)ptr,1,8*65536,fd)!=8*65536) {
				fprintf(stderr,"loading node: read error\n");
				return -1;
			}
			for (ci=0 ; ci<65536 ; ci++) {
				chunkid = get64bit(&chptr);
				if (chunkid>0) {
					printf("%016" PRIX64,chunkid);
				} else {
					printf("N");
				}
				printf(",");
			}
			ch-=65536;
		}

		if (fread((uint8_t*)ptr,1,8*ch+4*sessionids,fd)!=8*ch+4*sessionids) {
			fprintf(stderr,"loading node: read error\n");
			return -1;
		}

		while (ch>0) {
			chunkid = get64bit(&ptr);
			if (chunkid>0) {
				printf("%016" PRIX64,chunkid);
			} else {
				printf("N");
			}
			if (ch>1) {
				printf(",");
			}
			ch--;
		}
		printf(")|r:(");
		while (sessionids>0) {
			sessionid = get32bit(&ptr);
			printf("%" PRIu32,sessionid);
			if (sessionids>1) {
				printf(",");
			}
			sessionids--;
		}
		printf(")\n");
	} else {
		printf("\n");
	}

	return 0;
}

int fs_loadnodes(FILE *fd) {
	int s;
	do {
		s = fs_loadnode(fd);
		if (s<0) {
			return -1;
		}
	} while (s==0);
	return 0;
}

int fs_loadedges(FILE *fd) {
	int s;
	do {
		s = dump::fs_loadedge(fd);
		if (s<0) {
			return -1;
		}
	} while (s==0);
	return 0;
}

int fs_loadfree(FILE *fd, uint64_t section_size = 0) {
	uint8_t rbuff[8];
	const uint8_t *ptr;
	uint32_t t,nodeid,ftime;
	if (fread(rbuff,1,4,fd)!=4) {
		return -1;
	}
	ptr=rbuff;
	t = get32bit(&ptr);

	if (section_size && t != (section_size - 4) / 8) {
		t = (section_size - 4) / 8;
	}

	printf("# free nodes: %" PRIu32 "\n",t);
	while (t>0) {
		if (fread(rbuff,1,8,fd)!=8) {
			return -1;
		}
		ptr = rbuff;
		nodeid = get32bit(&ptr);
		ftime = get32bit(&ptr);
		printf("I|i:%10" PRIu32 "|f:%10" PRIu32 "\n", nodeid, ftime);
		t--;
	}
	return 0;
}

int hexdump(FILE *fd,uint64_t sleng) {
	uint8_t lbuff[32];
	uint32_t i;
	while (sleng>32) {
		if (fread(lbuff,1,32,fd)!=32) {
			return -1;
		}
		for (i=0 ; i<32 ; i++) {
			printf("%02" PRIX8 " ",lbuff[i]);
		}
		printf(" |");
		for (i=0 ; i<32 ; i++) {
			printf("%c",dispchar(lbuff[i]));
		}
		printf("|\n");
		sleng-=32;
	}
	if (sleng>0) {
		if (fread(lbuff,1,sleng,fd)!=(size_t)sleng) {
			return -1;
		}
		for (i=0 ; i<32 ; i++) {
			if (i<sleng) {
				printf("%02" PRIX8 " ",lbuff[i]);
			} else {
				printf("   ");
			}
		}
		printf(" |");
		for (i=0 ; i<32 ; i++) {
			if (i<sleng) {
				printf("%c",dispchar(lbuff[i]));
			} else {
				printf(" ");
			}
		}
		printf("|\n");
	}
	return 0;
}

int fs_load(FILE *fd) {
	uint32_t maxnodeid,nextsessionid;
	uint64_t version;
	uint8_t hdr[16];
	const uint8_t *ptr;
	if (fread(hdr,1,16,fd)!=16) {
		return -1;
	}
	ptr = hdr;
	maxnodeid = get32bit(&ptr);
	version = get64bit(&ptr);
	nextsessionid = get32bit(&ptr);

	printf("# maxnodeid: %" PRIu32 " ; version: %" PRIu64 " ; nextsessionid: %" PRIu32 "\n",maxnodeid,version,nextsessionid);

	printf("# -------------------------------------------------------------------\n");
	if (fs_loadnodes(fd)<0) {
		printf("error reading metadata (node)\n");
		return -1;
	}
	printf("# -------------------------------------------------------------------\n");
	if (fs_loadedges(fd)<0) {
		printf("error reading metadata (edge)\n");
		return -1;
	}
	printf("# -------------------------------------------------------------------\n");
	if (fs_loadfree(fd)<0) {
		printf("error reading metadata (free)\n");
		return -1;
	}
	printf("# -------------------------------------------------------------------\n");
	return 0;
}

int fs_load_acl(FILE *fd) {
	static std::vector<uint8_t> buffer;
	buffer.clear();

	try {
		// Read size of the entry
		uint32_t size = 0;
		buffer.resize(serializedSize(size));
		if (fread(buffer.data(), 1, buffer.size(), fd) != buffer.size()) {
			throw Exception(std::string("read error: ") + strerr(errno), LIZARDFS_ERROR_IO);
		}
		deserialize(buffer, size);
		if (size == 0) {
			// this is end marker
			return 1;
		} else if (size > 10000000) {
			throw Exception("strange size of entry: " + std::to_string(size),
			    LIZARDFS_ERROR_ERANGE);
		}

		// Read the entry
		buffer.resize(size);
		if (fread(buffer.data(), 1, buffer.size(), fd) != buffer.size()) {
			throw Exception(std::string("read error: ") + strerr(errno), LIZARDFS_ERROR_IO);
		}

		// Deserialize inode
		uint32_t inode;
		deserialize(buffer, inode);

		// Deserialize ACL
		RichACL acl;
		deserialize(buffer, inode, acl);
		printf("A|i: %10d: %s\n", inode, acl.toString().c_str());
		return 0;
	} catch (Exception &ex) {
		printf("Error loading acl: %s", ex.what());
		return -1;
	}
}

int fs_load_acls(FILE *fd) {
	int s;

	do {
		s = dump::fs_load_acl(fd);
		if (s < 0) {
			return -1;
		}
	} while (s == 0);

	return 0;
}

int fs_load_2x(FILE *fd, bool loadLockIds) {
	uint32_t maxnodeid,nextsessionid;
	uint64_t sleng;
	off_t offbegin;
	uint64_t version;
	uint8_t hdr[16];
	const uint8_t *ptr;
	if (fread(hdr,1,16,fd)!=16) {
		return -1;
	}
	ptr = hdr;
	maxnodeid = get32bit(&ptr);
	version = get64bit(&ptr);
	nextsessionid = get32bit(&ptr);

	printf("# maxnodeid: %" PRIu32 " ; version: %" PRIu64 " ; nextsessionid: %" PRIu32 "\n",maxnodeid,version,nextsessionid);

	while (1) {
		if (fread(hdr,1,16,fd)!=16) {
			printf("can't read section header\n");
			return -1;
		}
		if (memcmp(hdr,"[MFS EOF MARKER]",16)==0) {
			printf("# -------------------------------------------------------------------\n");
			printf("# LizardFS END OF FILE MARKER\n");
			printf("# -------------------------------------------------------------------\n");
			return 0;
		}
		ptr = hdr+8;
		sleng = get64bit(&ptr);
		offbegin = ftello(fd);
		printf("# -------------------------------------------------------------------\n");
		printf("# section header: %c%c%c%c%c%c%c%c (%02X%02X%02X%02X%02X%02X%02X%02X) ; length: %" PRIu64 "\n",dispchar(hdr[0]),dispchar(hdr[1]),dispchar(hdr[2]),dispchar(hdr[3]),dispchar(hdr[4]),dispchar(hdr[5]),dispchar(hdr[6]),dispchar(hdr[7]),hdr[0],hdr[1],hdr[2],hdr[3],hdr[4],hdr[5],hdr[6],hdr[7],sleng);
		if (memcmp(hdr,"NODE 1.0",8)==0) {
			if (dump::fs_loadnodes(fd)<0) {
				printf("error reading metadata (NODE 1.0)\n");
				return -1;
			}
		} else if (memcmp(hdr,"EDGE 1.0",8)==0) {
			if (dump::fs_loadedges(fd)<0) {
				printf("error reading metadata (EDGE 1.0)\n");
				return -1;
			}
		} else if (memcmp(hdr,"FREE 1.0",8)==0) {
			if (dump::fs_loadfree(fd, sleng)<0) {
				printf("error reading metadata (FREE 1.0)\n");
				return -1;
			}
		} else if (memcmp(hdr,"CHNK 1.0",8)==0) {
			if (dump::chunk_load(fd, loadLockIds) < 0) {
				printf("error reading metadata (CHNK 1.0)\n");
				return -1;
			}
		} else if (memcmp(hdr,"ACLS 1.2",8)==0) {
			if (dump::fs_load_acls(fd) < 0) {
				printf("error reading metadata (ACLS 1.2)\n");
				return -1;
			}
		} else {
			printf("unknown file part\n");
			if (hexdump(fd,sleng)<0) {
				return -1;
			}
		}
		if ((off_t)(offbegin+sleng)!=ftello(fd)) {
			fprintf(stderr,"some data in this section have not been read - file corrupted\n");
			return -1;
		}
	}
	return 0;
}

inline int fs_load_20(FILE *fd) {
	return fs_load_2x(fd, false);
}

inline int fs_load_29(FILE *fd) {
	return fs_load_2x(fd, true);
}

int fs_loadall(const char *fname) {
	FILE *fd;
	uint8_t hdr[8];

	fd = fopen(fname,"r");

	if (fd==NULL) {
		printf("can't open metadata file\n");
		return -1;
	}
	if (fread(hdr,1,8,fd)!=8) {
		printf("can't read metadata header\n");
		fclose(fd);
		return -1;
	}
	printf("# header: %c%c%c%c%c%c%c%c (%02X%02X%02X%02X%02X%02X%02X%02X)\n",dispchar(hdr[0]),dispchar(hdr[1]),dispchar(hdr[2]),dispchar(hdr[3]),dispchar(hdr[4]),dispchar(hdr[5]),dispchar(hdr[6]),dispchar(hdr[7]),hdr[0],hdr[1],hdr[2],hdr[3],hdr[4],hdr[5],hdr[6],hdr[7]);
	if (memcmp(hdr,MFSSIGNATURE "M 1.5",8)==0 || memcmp(hdr,MFSSIGNATURE "M 1.6",8)==0) {
		bool loadLockIds = (hdr[7] == '6');
		if (fs_load(fd) < 0) {
			printf("error reading metadata (structure)\n");
			fclose(fd);
			return -1;
		}
		if (dump::chunk_load(fd, loadLockIds) < 0) {
			printf("error reading metadata (chunks)\n");
			fclose(fd);
			return -1;
		}
	} else if (memcmp(hdr,MFSSIGNATURE "M 2.0",8) == 0) {
		if (fs_load_20(fd) < 0) {
			fclose(fd);
			return -1;
		}
	/* Note LIZARDFSSIGNATURE instead of MFSSIGNATURE! */
	} else if (memcmp(hdr,LIZARDFSSIGNATURE "M 2.9",8) == 0) {
		if (fs_load_29(fd) < 0) {
			fclose(fd);
			return -1;
		}
	} else {
		printf("wrong metadata header (old version ?)\n");
		fclose(fd);
		return -1;
	}
	if (ferror(fd)!=0) {
		printf("error reading metadata\n");
		fclose(fd);
		return -1;
	}
	fclose(fd);
	return 0;
}

}

int main(int argc,char **argv) {
	if (argc!=2) {
		printf("usage: %s metadata_file\n",argv[0]);
		return 1;
	}
	return (dump::fs_loadall(argv[1])<0)?1:0;
}
