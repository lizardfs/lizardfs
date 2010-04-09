/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA.

   This file is part of MooseFS.

   MooseFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   MooseFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with MooseFS.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

#include "MFSCommunication.h"
#include "filesystem.h"

#define EAT(clptr,vno,c) { \
	if (*(clptr)!=(c)) { \
		printf("%"PRIu64": '%c' expected\n",(vno),(c)); \
		return 1; \
	} \
	(clptr)++; \
}

#define GETNAME(name,clptr,vno,c) { \
	uint32_t _tmp_i; \
	char _tmp_c,_tmp_h1,_tmp_h2; \
	memset((void*)(name),0,256); \
	_tmp_i = 0; \
	while ((_tmp_c=*((clptr)++))!=c && _tmp_i<255) { \
		if (_tmp_c=='%') { \
			_tmp_h1 = *((clptr)++); \
			_tmp_h2 = *((clptr)++); \
			if (_tmp_h1>='0' && _tmp_h1<='9') { \
				_tmp_h1-='0'; \
			} else if (_tmp_h1>='A' && _tmp_h1<='F') { \
				_tmp_h1-=('A'-10); \
			} else { \
				printf("%"PRIu64": hex expected\n",(vno)); \
				return 1; \
			} \
			if (_tmp_h2>='0' && _tmp_h2<='9') { \
				_tmp_h2-='0'; \
			} else if (_tmp_h2>='A' && _tmp_h2<='F') { \
				_tmp_h2-=('A'-10); \
			} else { \
				printf("%"PRIu64": hex expected\n",(vno)); \
				return 1; \
			} \
			_tmp_c = _tmp_h1*16+_tmp_h2; \
		} \
		name[_tmp_i++] = _tmp_c; \
	} \
	(clptr)--; \
	name[_tmp_i]=0; \
}

#define GETPATH(path,size,clptr,vno,c) { \
	uint32_t _tmp_i; \
	char _tmp_c,_tmp_h1,_tmp_h2; \
	_tmp_i = 0; \
	while ((_tmp_c=*((clptr)++))!=c) { \
		if (_tmp_c=='%') { \
			_tmp_h1 = *((clptr)++); \
			_tmp_h2 = *((clptr)++); \
			if (_tmp_h1>='0' && _tmp_h1<='9') { \
				_tmp_h1-='0'; \
			} else if (_tmp_h1>='A' && _tmp_h1<='F') { \
				_tmp_h1-=('A'-10); \
			} else { \
				printf("%"PRIu64": hex expected\n",(vno)); \
				return 1; \
			} \
			if (_tmp_h2>='0' && _tmp_h2<='9') { \
				_tmp_h2-='0'; \
			} else if (_tmp_h2>='A' && _tmp_h2<='F') { \
				_tmp_h2-=('A'-10); \
			} else { \
				printf("%"PRIu64": hex expected\n",(vno)); \
				return 1; \
			} \
			_tmp_c = _tmp_h1*16+_tmp_h2; \
		} \
		if ((_tmp_i)>=(size)) { \
			(size) = _tmp_i+1000; \
			if ((path)==NULL) { \
				(path) = malloc(size); \
			} else { \
				(path) = realloc((path),(size)); \
			} \
		} \
		path[_tmp_i++]=_tmp_c; \
	} \
	if ((_tmp_i)>=(size)) { \
		(size) = _tmp_i+1000; \
		if ((path)==NULL) { \
			(path) = malloc(size); \
		} else { \
			(path) = realloc((path),(size)); \
		} \
	} \
	(clptr)--; \
	path[_tmp_i]=0; \
}

#define GETU32(data,clptr) (data)=strtoul(clptr,&clptr,10)
#define GETU64(data,clptr) (data)=strtoull(clptr,&clptr,10)

uint8_t do_access(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode;
	EAT(ptr,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,lv,')');
	return fs_access(ts,inode);
}

uint8_t do_append(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,inode_src;
	EAT(ptr,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,lv,',');
	GETU32(inode_src,ptr);
	EAT(ptr,lv,')');
	return fs_append(ts,inode,inode_src);
}

uint8_t do_aquire(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,cuid;
	(void)ts;
	EAT(ptr,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,lv,',');
	GETU32(cuid,ptr);
	EAT(ptr,lv,')');
	return fs_aquire(inode,cuid);
}

uint8_t do_attr(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,mode,uid,gid,atime,mtime;
	EAT(ptr,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,lv,',');
	GETU32(mode,ptr);
	EAT(ptr,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,lv,',');
	GETU32(gid,ptr);
	EAT(ptr,lv,',');
	GETU32(atime,ptr);
	EAT(ptr,lv,',');
	GETU32(mtime,ptr);
	EAT(ptr,lv,')');
	return fs_attr(ts,inode,mode,uid,gid,atime,mtime);
}

/*
int do_copy(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,parent;
	uint8_t name[256];
	EAT(ptr,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,lv,',');
	GETU32(parent,ptr);
	EAT(ptr,lv,',');
	GETNAME(name,ptr,lv,')');
	EAT(ptr,lv,')');
	return fs_copy(ts,inode,parent,strlen(name),name);
}
*/

uint8_t do_create(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t parent,mode,uid,gid,rdev,inode;
	uint8_t type,name[256];
	EAT(ptr,lv,'(');
	GETU32(parent,ptr);
	EAT(ptr,lv,',');
	GETNAME(name,ptr,lv,',');
	EAT(ptr,lv,',');
	type = *ptr;
	ptr++;
	EAT(ptr,lv,',');
	GETU32(mode,ptr);
	EAT(ptr,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,lv,',');
	GETU32(gid,ptr);
	EAT(ptr,lv,',');
	GETU32(rdev,ptr);
	EAT(ptr,lv,')');
	EAT(ptr,lv,':');
	GETU32(inode,ptr);
	return fs_create(ts,parent,strlen((char*)name),name,type,mode,uid,gid,rdev,inode);
}

uint8_t do_session(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t cuid;
	(void)ts;
	EAT(ptr,lv,'(');
	EAT(ptr,lv,')');
	EAT(ptr,lv,':');
	GETU32(cuid,ptr);
	return fs_session(cuid);
}

uint8_t do_emptytrash(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t reservedinodes,freeinodes;
	EAT(ptr,lv,'(');
	EAT(ptr,lv,')');
	EAT(ptr,lv,':');
	GETU32(freeinodes,ptr);
	EAT(ptr,lv,',');
	GETU32(reservedinodes,ptr);
	return fs_emptytrash(ts,freeinodes,reservedinodes);
}

uint8_t do_emptyreserved(uint64_t lv,uint32_t ts,char* ptr) {
	uint32_t freeinodes;
	EAT(ptr,lv,'(');
	EAT(ptr,lv,')');
	EAT(ptr,lv,':');
	GETU32(freeinodes,ptr);
	return fs_emptyreserved(ts,freeinodes);
}

uint8_t do_freeinodes(uint64_t lv,uint32_t ts,char* ptr) {
	uint32_t freeinodes;
	EAT(ptr,lv,'(');
	EAT(ptr,lv,')');
	EAT(ptr,lv,':');
	GETU32(freeinodes,ptr);
	return fs_freeinodes(ts,freeinodes);
}

uint8_t do_incversion(uint64_t lv,uint32_t ts,char *ptr) {
	uint64_t chunkid;
	(void)ts;
	EAT(ptr,lv,'(');
	GETU64(chunkid,ptr);
	EAT(ptr,lv,')');
	return fs_incversion(chunkid);
}

uint8_t do_link(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,parent;
	uint8_t name[256];
	EAT(ptr,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,lv,',');
	GETU32(parent,ptr);
	EAT(ptr,lv,',');
	GETNAME(name,ptr,lv,')');
	EAT(ptr,lv,')');
	return fs_link(ts,inode,parent,strlen((char*)name),name);
}

uint8_t do_length(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode;
	uint64_t length;
	EAT(ptr,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,lv,',');
	GETU64(length,ptr);
	EAT(ptr,lv,')');
	return fs_length(ts,inode,length);
}

uint8_t do_move(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,parent_src,parent_dst;
	uint8_t name_src[256],name_dst[256];
	EAT(ptr,lv,'(');
	GETU32(parent_src,ptr);
	EAT(ptr,lv,',');
	GETNAME(name_src,ptr,lv,',');
	EAT(ptr,lv,',');
	GETU32(parent_dst,ptr);
	EAT(ptr,lv,',');
	GETNAME(name_dst,ptr,lv,')');
	EAT(ptr,lv,')');
	EAT(ptr,lv,':');
	GETU32(inode,ptr);
	return fs_move(ts,parent_src,strlen((char*)name_src),name_src,parent_dst,strlen((char*)name_dst),name_dst,inode);
}

uint8_t do_purge(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode;
	EAT(ptr,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,lv,')');
	return fs_purge(ts,inode);
}

/*
uint8_t do_reinit(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,indx;
	uint64_t chunkid;
	EAT(ptr,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,lv,',');
	GETU32(indx,ptr);
	EAT(ptr,lv,')');
	EAT(ptr,lv,':');
	GETU64(chunkid,ptr);
	return fs_reinit(ts,inode,indx,chunkid);
}
*/
uint8_t do_release(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,cuid;
	(void)ts;
	EAT(ptr,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,lv,',');
	GETU32(cuid,ptr);
	EAT(ptr,lv,')');
	return fs_release(inode,cuid);
}

uint8_t do_repair(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,indx;
	uint32_t version;
	EAT(ptr,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,lv,',');
	GETU32(indx,ptr);
	EAT(ptr,lv,')');
	EAT(ptr,lv,':');
	GETU32(version,ptr);
	return fs_repair(ts,inode,indx,version);
}
/*
int do_remove(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode;
	EAT(ptr,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,lv,')');
	return fs_remove(ts,inode);
}
*/
uint8_t do_seteattr(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,uid,ci,nci,npi;
	uint8_t eattr,smode;
	EAT(ptr,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,lv,',');
	GETU32(eattr,ptr);
	EAT(ptr,lv,',');
	GETU32(smode,ptr);
	EAT(ptr,lv,')');
	EAT(ptr,lv,':');
	GETU32(ci,ptr);
	EAT(ptr,lv,',');
	GETU32(nci,ptr);
	EAT(ptr,lv,',');
	GETU32(npi,ptr);
	return fs_seteattr(ts,inode,uid,eattr,smode,ci,nci,npi);
}

uint8_t do_setgoal(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,uid,ci,nci,npi;
	uint8_t goal,smode;
	EAT(ptr,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,lv,',');
	GETU32(goal,ptr);
	EAT(ptr,lv,',');
	GETU32(smode,ptr);
	EAT(ptr,lv,')');
	EAT(ptr,lv,':');
	GETU32(ci,ptr);
	EAT(ptr,lv,',');
	GETU32(nci,ptr);
	EAT(ptr,lv,',');
	GETU32(npi,ptr);
	return fs_setgoal(ts,inode,uid,goal,smode,ci,nci,npi);
}

uint8_t do_setpath(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode;
	static uint8_t *path;
	static uint32_t pathsize;
	(void)ts;
	EAT(ptr,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,lv,',');
	GETPATH(path,pathsize,ptr,lv,')');
	EAT(ptr,lv,')');
	return fs_setpath(inode,path);
}

uint8_t do_settrashtime(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,uid,ci,nci,npi;
	uint32_t trashtime;
	uint8_t smode;
	EAT(ptr,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,lv,',');
	GETU32(trashtime,ptr);
	EAT(ptr,lv,',');
	GETU32(smode,ptr);
	EAT(ptr,lv,')');
	EAT(ptr,lv,':');
	GETU32(ci,ptr);
	EAT(ptr,lv,',');
	GETU32(nci,ptr);
	EAT(ptr,lv,',');
	GETU32(npi,ptr);
	return fs_settrashtime(ts,inode,uid,trashtime,smode,ci,nci,npi);
}

uint8_t do_snapshot(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,parent,canoverwrite;
	uint8_t name[256];
	EAT(ptr,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,lv,',');
	GETU32(parent,ptr);
	EAT(ptr,lv,',');
	GETNAME(name,ptr,lv,',');
	EAT(ptr,lv,',');
	GETU32(canoverwrite,ptr);
	EAT(ptr,lv,')');
	return fs_snapshot(ts,inode,parent,strlen((char*)name),name,canoverwrite);
}

uint8_t do_symlink(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t parent,uid,gid,inode;
	uint8_t name[256];
	static uint8_t *path;
	static uint32_t pathsize;
	EAT(ptr,lv,'(');
	GETU32(parent,ptr);
	EAT(ptr,lv,',');
	GETNAME(name,ptr,lv,',');
	EAT(ptr,lv,',');
	GETPATH(path,pathsize,ptr,lv,',');
	EAT(ptr,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,lv,',');
	GETU32(gid,ptr);
	EAT(ptr,lv,')');
	EAT(ptr,lv,':');
	GETU32(inode,ptr);
	return fs_symlink(ts,parent,strlen((char*)name),name,path,uid,gid,inode);
}

uint8_t do_undel(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode;
	EAT(ptr,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,lv,')');
	return fs_undel(ts,inode);
}

uint8_t do_unlink(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,parent;
	uint8_t name[256];
	EAT(ptr,lv,'(');
	GETU32(parent,ptr);
	EAT(ptr,lv,',');
	GETNAME(name,ptr,lv,')');
	EAT(ptr,lv,')');
	EAT(ptr,lv,':');
	GETU32(inode,ptr);
	return fs_unlink(ts,parent,strlen((char*)name),name,inode);
}

uint8_t do_unlock(uint64_t lv,uint32_t ts,char *ptr) {
	uint64_t chunkid;
	(void)ts;
	EAT(ptr,lv,'(');
	GETU64(chunkid,ptr);
	EAT(ptr,lv,')');
	return fs_unlock(chunkid);
}

uint8_t do_trunc(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,indx;
	uint64_t chunkid;
	EAT(ptr,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,lv,',');
	GETU32(indx,ptr);
	EAT(ptr,lv,')');
	EAT(ptr,lv,':');
	GETU64(chunkid,ptr);
	return fs_trunc(ts,inode,indx,chunkid);
}

uint8_t do_write(uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,indx,opflag;
	uint64_t chunkid;
	EAT(ptr,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,lv,',');
	GETU32(indx,ptr);
	if (*ptr==',') {
		EAT(ptr,lv,',');
		GETU32(opflag,ptr);
	} else {
		opflag=1;
	}
	EAT(ptr,lv,')');
	EAT(ptr,lv,':');
	GETU64(chunkid,ptr);
	return fs_write(ts,inode,indx,opflag,chunkid);
}

int restore(const char *rfname) {
	FILE *fd;
	char buff[10000];
	char *ptr;
	uint64_t v,lv;
	uint32_t ts;
	uint8_t status;
	char* errormsgs[]={ ERROR_STRINGS };

	v = fs_getversion();
	lv = 0;

	printf("meta data version: %"PRIu64"\n",v);

	fd = fopen(rfname,"r");
	if (fd==NULL) {
		printf("can't open changemeta file: %s\n",rfname);
		return 1;
	}
	while (fgets(buff,10000,fd)) {
		ptr = buff;
		GETU64(lv,ptr);
		if (lv<v) {
			// skip
		} else {
			status = ERROR_MISMATCH;
			EAT(ptr,lv,':');
			EAT(ptr,lv,' ');
			GETU32(ts,ptr);
			EAT(ptr,lv,'|');
			switch (*ptr) {
			case 'A':
				if (strncmp(ptr,"ACCESS",6)==0) {
					status = do_access(lv,ts,ptr+6);
				} else if (strncmp(ptr,"ATTR",4)==0) {
					status = do_attr(lv,ts,ptr+4);
				} else if (strncmp(ptr,"APPEND",6)==0) {
					status = do_append(lv,ts,ptr+6);
				} else if (strncmp(ptr,"AQUIRE",6)==0) {
					status = do_aquire(lv,ts,ptr+6);
				} else {
					printf("%"PRIu64": unknown entry '%s'\n",lv,ptr);
				}
				break;
			case 'C':
				if (strncmp(ptr,"CREATE",6)==0) {
					status = do_create(lv,ts,ptr+6);
				} else if (strncmp(ptr,"CUSTOMER",8)==0) {	// deprecated
					status = do_session(lv,ts,ptr+8);
				} else {
					printf("%"PRIu64": unknown entry '%s'\n",lv,ptr);
				}
				break;
			case 'E':
				if (strncmp(ptr,"EMPTYTRASH",10)==0) {
					status = do_emptytrash(lv,ts,ptr+10);
				} else if (strncmp(ptr,"EMPTYRESERVED",13)==0) {
					status = do_emptyreserved(lv,ts,ptr+13);
				} else {
					printf("%"PRIu64": unknown entry '%s'\n",lv,ptr);
				}
				break;
			case 'F':
				if (strncmp(ptr,"FREEINODES",10)==0) {
					status = do_freeinodes(lv,ts,ptr+10);
				} else {
					printf("%"PRIu64": unknown entry '%s'\n",lv,ptr);
				}
				break;
			case 'I':
				if (strncmp(ptr,"INCVERSION",10)==0) {
					status = do_incversion(lv,ts,ptr+10);
				} else {
					printf("%"PRIu64": unknown entry '%s'\n",lv,ptr);
				}
				break;
			case 'L':
				if (strncmp(ptr,"LENGTH",6)==0) {
					status = do_length(lv,ts,ptr+6);
				} else if (strncmp(ptr,"LINK",4)==0) {
					status = do_link(lv,ts,ptr+4);
				} else {
					printf("%"PRIu64": unknown entry '%s'\n",lv,ptr);
				}
				break;
			case 'M':
				if (strncmp(ptr,"MOVE",4)==0) {
					status = do_move(lv,ts,ptr+4);
				} else {
					printf("%"PRIu64": unknown entry '%s'\n",lv,ptr);
				}
				break;
			case 'P':
				if (strncmp(ptr,"PURGE",5)==0) {
					status = do_purge(lv,ts,ptr+5);
				} else {
					printf("%"PRIu64": unknown entry '%s'\n",lv,ptr);
				}
				break;
			case 'R':
				if (strncmp(ptr,"RELEASE",7)==0) {
					status = do_release(lv,ts,ptr+7);
				} else if (strncmp(ptr,"REPAIR",6)==0) {
					status = do_repair(lv,ts,ptr+6);
				} else {
					printf("%"PRIu64": unknown entry '%s'\n",lv,ptr);
				}
				break;
			case 'S':
				if (strncmp(ptr,"SETEATTR",8)==0) {
					status = do_seteattr(lv,ts,ptr+8);
				} else if (strncmp(ptr,"SETGOAL",7)==0) {
					status = do_setgoal(lv,ts,ptr+7);
				} else if (strncmp(ptr,"SETPATH",7)==0) {
					status = do_setpath(lv,ts,ptr+7);
				} else if (strncmp(ptr,"SETTRASHTIME",12)==0) {
					status = do_settrashtime(lv,ts,ptr+12);
				} else if (strncmp(ptr,"SNAPSHOT",8)==0) {
					status = do_snapshot(lv,ts,ptr+8);
				} else if (strncmp(ptr,"SYMLINK",7)==0) {
					status = do_symlink(lv,ts,ptr+7);
				} else if (strncmp(ptr,"SESSION",7)==0) {
					status = do_session(lv,ts,ptr+7);
				} else {
					printf("%"PRIu64": unknown entry '%s'\n",lv,ptr);
				}
				break;
			case 'T':
				if (strncmp(ptr,"TRUNC",5)==0) {
					status = do_trunc(lv,ts,ptr+5);
				} else {
					printf("%"PRIu64": unknown entry '%s'\n",lv,ptr);
				}
				break;
			case 'U':
				if (strncmp(ptr,"UNLINK",6)==0) {
					status = do_unlink(lv,ts,ptr+6);
				} else if (strncmp(ptr,"UNDEL",5)==0) {
					status = do_undel(lv,ts,ptr+5);
				} else if (strncmp(ptr,"UNLOCK",6)==0) {
					status = do_unlock(lv,ts,ptr+6);
				} else {
					printf("%"PRIu64": unknown entry '%s'\n",lv,ptr);
				}
				break;
			case 'W':
				if (strncmp(ptr,"WRITE",5)==0) {
					status = do_write(lv,ts,ptr+5);
				} else {
					printf("%"PRIu64": unknown entry '%s'\n",lv,ptr);
				}
				break;
			default:
				printf("%"PRIu64": unknown entry '%s'\n",lv,ptr);
			}
			if (status!=STATUS_OK) {
				printf("%"PRIu64": error: %"PRIu8" (%s)\n",lv,status,errormsgs[status]);
				return 1;
			}
			v = fs_getversion();
			if (lv+1!=v) {
				printf("%"PRIu64": version mismatch\n",lv);
				return 1;
			}
		}
	}
	fclose(fd);
	printf("version after applying changelog: %"PRIu64"\n",v);
	return 0;
}
