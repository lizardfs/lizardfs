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

#define EAT(clptr,fn,vno,c) { \
	if (*(clptr)!=(c)) { \
		printf("%s:%"PRIu64": '%c' expected\n",(fn),(vno),(c)); \
		return -1; \
	} \
	(clptr)++; \
}

#define GETNAME(name,clptr,fn,vno,c) { \
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
				printf("%s:%"PRIu64": hex expected\n",(fn),(vno)); \
				return -1; \
			} \
			if (_tmp_h2>='0' && _tmp_h2<='9') { \
				_tmp_h2-='0'; \
			} else if (_tmp_h2>='A' && _tmp_h2<='F') { \
				_tmp_h2-=('A'-10); \
			} else { \
				printf("%s:%"PRIu64": hex expected\n",(fn),(vno)); \
				return -1; \
			} \
			_tmp_c = _tmp_h1*16+_tmp_h2; \
		} \
		name[_tmp_i++] = _tmp_c; \
	} \
	(clptr)--; \
	name[_tmp_i]=0; \
}

#define GETPATH(path,size,clptr,fn,vno,c) { \
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
				printf("%s:%"PRIu64": hex expected\n",(fn),(vno)); \
				return -1; \
			} \
			if (_tmp_h2>='0' && _tmp_h2<='9') { \
				_tmp_h2-='0'; \
			} else if (_tmp_h2>='A' && _tmp_h2<='F') { \
				_tmp_h2-=('A'-10); \
			} else { \
				printf("%s:%"PRIu64": hex expected\n",(fn),(vno)); \
				return -1; \
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

int do_access(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,')');
	return fs_access(ts,inode);
}

int do_append(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,inode_src;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(inode_src,ptr);
	EAT(ptr,filename,lv,')');
	return fs_append(ts,inode,inode_src);
}

int do_acquire(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,cuid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(cuid,ptr);
	EAT(ptr,filename,lv,')');
	return fs_acquire(inode,cuid);
}

int do_attr(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,mode,uid,gid,atime,mtime;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(mode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(gid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(atime,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(mtime,ptr);
	EAT(ptr,filename,lv,')');
	return fs_attr(ts,inode,mode,uid,gid,atime,mtime);
}

/*
int do_copy(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,parent;
	uint8_t name[256];
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(parent,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	return fs_copy(ts,inode,parent,strlen(name),name);
}
*/

int do_create(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t parent,mode,uid,gid,rdev,inode;
	uint8_t type,name[256];
	EAT(ptr,filename,lv,'(');
	GETU32(parent,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	type = *ptr;
	ptr++;
	EAT(ptr,filename,lv,',');
	GETU32(mode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(gid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(rdev,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(inode,ptr);
	return fs_create(ts,parent,strlen((char*)name),name,type,mode,uid,gid,rdev,inode);
}

int do_session(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t cuid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(cuid,ptr);
	return fs_session(cuid);
}

int do_emptytrash(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t reservedinodes,freeinodes;
	EAT(ptr,filename,lv,'(');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(freeinodes,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(reservedinodes,ptr);
	return fs_emptytrash(ts,freeinodes,reservedinodes);
}

int do_emptyreserved(const char *filename,uint64_t lv,uint32_t ts,char* ptr) {
	uint32_t freeinodes;
	EAT(ptr,filename,lv,'(');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(freeinodes,ptr);
	return fs_emptyreserved(ts,freeinodes);
}

int do_freeinodes(const char *filename,uint64_t lv,uint32_t ts,char* ptr) {
	uint32_t freeinodes;
	EAT(ptr,filename,lv,'(');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(freeinodes,ptr);
	return fs_freeinodes(ts,freeinodes);
}

int do_incversion(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint64_t chunkid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU64(chunkid,ptr);
	EAT(ptr,filename,lv,')');
	return fs_incversion(chunkid);
}

int do_link(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,parent;
	uint8_t name[256];
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(parent,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	return fs_link(ts,inode,parent,strlen((char*)name),name);
}

int do_length(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode;
	uint64_t length;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(length,ptr);
	EAT(ptr,filename,lv,')');
	return fs_length(ts,inode,length);
}

int do_move(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,parent_src,parent_dst;
	uint8_t name_src[256],name_dst[256];
	EAT(ptr,filename,lv,'(');
	GETU32(parent_src,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name_src,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	GETU32(parent_dst,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name_dst,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(inode,ptr);
	return fs_move(ts,parent_src,strlen((char*)name_src),name_src,parent_dst,strlen((char*)name_dst),name_dst,inode);
}

int do_purge(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,')');
	return fs_purge(ts,inode);
}

int do_quota(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,stimestamp,sinodes,hinodes;
	uint64_t slength,ssize,srealsize;
	uint64_t hlength,hsize,hrealsize;
	uint32_t flags,exceeded;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(exceeded,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(flags,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(stimestamp,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(sinodes,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(hinodes,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(slength,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(hlength,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(ssize,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(hsize,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(srealsize,ptr);
	EAT(ptr,filename,lv,',');
	GETU64(hrealsize,ptr);
	EAT(ptr,filename,lv,')');
	return fs_quota(ts,inode,exceeded,flags,stimestamp,sinodes,hinodes,slength,hlength,ssize,hsize,srealsize,hrealsize);
}

/*
int do_reinit(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,indx;
	uint64_t chunkid;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(indx,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU64(chunkid,ptr);
	return fs_reinit(ts,inode,indx,chunkid);
}
*/
int do_release(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,cuid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(cuid,ptr);
	EAT(ptr,filename,lv,')');
	return fs_release(inode,cuid);
}

int do_repair(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,indx;
	uint32_t version;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(indx,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(version,ptr);
	return fs_repair(ts,inode,indx,version);
}
/*
int do_remove(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,')');
	return fs_remove(ts,inode);
}
*/
int do_seteattr(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,uid,ci,nci,npi;
	uint8_t eattr,smode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(eattr,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(smode,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(ci,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(nci,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(npi,ptr);
	return fs_seteattr(ts,inode,uid,eattr,smode,ci,nci,npi);
}

int do_setgoal(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
#if VERSHEX>=0x010700
	uint32_t inode,uid,ci,nci,npi,qei;
#else
	uint32_t inode,uid,ci,nci,npi;
#endif
	uint8_t goal,smode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(goal,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(smode,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(ci,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(nci,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(npi,ptr);
#if VERSHEX>=0x010700
	if (*ptr==',') {
		EAT(ptr,filename,lv,',');
		GETU32(qei,ptr);
	} else {
		qei = UINT32_C(0xFFFFFFFF);
	}
	return fs_setgoal(ts,inode,uid,goal,smode,ci,nci,npi,qei);
#else
	return fs_setgoal(ts,inode,uid,goal,smode,ci,nci,npi);
#endif
}

int do_setpath(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode;
	static uint8_t *path;
	static uint32_t pathsize;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETPATH(path,pathsize,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	return fs_setpath(inode,path);
}

int do_settrashtime(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,uid,ci,nci,npi;
	uint32_t trashtime;
	uint8_t smode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(trashtime,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(smode,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(ci,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(nci,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(npi,ptr);
	return fs_settrashtime(ts,inode,uid,trashtime,smode,ci,nci,npi);
}

int do_snapshot(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,parent,canoverwrite;
	uint8_t name[256];
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(parent,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	GETU32(canoverwrite,ptr);
	EAT(ptr,filename,lv,')');
	return fs_snapshot(ts,inode,parent,strlen((char*)name),name,canoverwrite);
}

int do_symlink(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t parent,uid,gid,inode;
	uint8_t name[256];
	static uint8_t *path;
	static uint32_t pathsize;
	EAT(ptr,filename,lv,'(');
	GETU32(parent,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	GETPATH(path,pathsize,ptr,filename,lv,',');
	EAT(ptr,filename,lv,',');
	GETU32(uid,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(gid,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(inode,ptr);
	return fs_symlink(ts,parent,strlen((char*)name),name,path,uid,gid,inode);
}

int do_undel(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,')');
	return fs_undel(ts,inode);
}

int do_unlink(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,parent;
	uint8_t name[256];
	EAT(ptr,filename,lv,'(');
	GETU32(parent,ptr);
	EAT(ptr,filename,lv,',');
	GETNAME(name,ptr,filename,lv,')');
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU32(inode,ptr);
	return fs_unlink(ts,parent,strlen((char*)name),name,inode);
}

int do_unlock(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint64_t chunkid;
	(void)ts;
	EAT(ptr,filename,lv,'(');
	GETU64(chunkid,ptr);
	EAT(ptr,filename,lv,')');
	return fs_unlock(chunkid);
}

int do_trunc(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,indx;
	uint64_t chunkid;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(indx,ptr);
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU64(chunkid,ptr);
	return fs_trunc(ts,inode,indx,chunkid);
}

int do_write(const char *filename,uint64_t lv,uint32_t ts,char *ptr) {
	uint32_t inode,indx,opflag;
	uint64_t chunkid;
	EAT(ptr,filename,lv,'(');
	GETU32(inode,ptr);
	EAT(ptr,filename,lv,',');
	GETU32(indx,ptr);
	if (*ptr==',') {
		EAT(ptr,filename,lv,',');
		GETU32(opflag,ptr);
	} else {
		opflag=1;
	}
	EAT(ptr,filename,lv,')');
	EAT(ptr,filename,lv,':');
	GETU64(chunkid,ptr);
	return fs_write(ts,inode,indx,opflag,chunkid);
}


int restore_line(const char *filename,uint64_t lv,char *line) {
	char *ptr;
	uint32_t ts;
	int status;
	char* errormsgs[]={ ERROR_STRINGS };

	status = ERROR_MISMATCH;
	ptr = line;

	EAT(ptr,filename,lv,':');
	EAT(ptr,filename,lv,' ');
	GETU32(ts,ptr);
	EAT(ptr,filename,lv,'|');
	switch (*ptr) {
		case 'A':
			if (strncmp(ptr,"ACCESS",6)==0) {
				status = do_access(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"ATTR",4)==0) {
				status = do_attr(filename,lv,ts,ptr+4);
			} else if (strncmp(ptr,"APPEND",6)==0) {
				status = do_append(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"ACQUIRE",7)==0) {
				status = do_acquire(filename,lv,ts,ptr+7);
			} else if (strncmp(ptr,"AQUIRE",6)==0) {
				status = do_acquire(filename,lv,ts,ptr+6);
			} else {
				printf("%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'C':
			if (strncmp(ptr,"CREATE",6)==0) {
				status = do_create(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"CUSTOMER",8)==0) {	// deprecated
				status = do_session(filename,lv,ts,ptr+8);
			} else {
				printf("%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'E':
			if (strncmp(ptr,"EMPTYTRASH",10)==0) {
				status = do_emptytrash(filename,lv,ts,ptr+10);
			} else if (strncmp(ptr,"EMPTYRESERVED",13)==0) {
				status = do_emptyreserved(filename,lv,ts,ptr+13);
			} else {
				printf("%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'F':
			if (strncmp(ptr,"FREEINODES",10)==0) {
				status = do_freeinodes(filename,lv,ts,ptr+10);
			} else {
				printf("%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'I':
			if (strncmp(ptr,"INCVERSION",10)==0) {
				status = do_incversion(filename,lv,ts,ptr+10);
			} else {
				printf("%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'L':
			if (strncmp(ptr,"LENGTH",6)==0) {
				status = do_length(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"LINK",4)==0) {
				status = do_link(filename,lv,ts,ptr+4);
			} else {
				printf("%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'M':
			if (strncmp(ptr,"MOVE",4)==0) {
				status = do_move(filename,lv,ts,ptr+4);
			} else {
				printf("%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'P':
			if (strncmp(ptr,"PURGE",5)==0) {
				status = do_purge(filename,lv,ts,ptr+5);
			} else {
				printf("%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'Q':
			if (strncmp(ptr,"QUOTA",5)==0) {
				status = do_quota(filename,lv,ts,ptr+5);
			}
			break;
		case 'R':
			if (strncmp(ptr,"RELEASE",7)==0) {
				status = do_release(filename,lv,ts,ptr+7);
			} else if (strncmp(ptr,"REPAIR",6)==0) {
				status = do_repair(filename,lv,ts,ptr+6);
			} else {
				printf("%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'S':
			if (strncmp(ptr,"SETEATTR",8)==0) {
				status = do_seteattr(filename,lv,ts,ptr+8);
			} else if (strncmp(ptr,"SETGOAL",7)==0) {
				status = do_setgoal(filename,lv,ts,ptr+7);
			} else if (strncmp(ptr,"SETPATH",7)==0) {
				status = do_setpath(filename,lv,ts,ptr+7);
			} else if (strncmp(ptr,"SETTRASHTIME",12)==0) {
				status = do_settrashtime(filename,lv,ts,ptr+12);
			} else if (strncmp(ptr,"SNAPSHOT",8)==0) {
				status = do_snapshot(filename,lv,ts,ptr+8);
			} else if (strncmp(ptr,"SYMLINK",7)==0) {
				status = do_symlink(filename,lv,ts,ptr+7);
			} else if (strncmp(ptr,"SESSION",7)==0) {
				status = do_session(filename,lv,ts,ptr+7);
			} else {
				printf("%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'T':
			if (strncmp(ptr,"TRUNC",5)==0) {
				status = do_trunc(filename,lv,ts,ptr+5);
			} else {
				printf("%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'U':
			if (strncmp(ptr,"UNLINK",6)==0) {
				status = do_unlink(filename,lv,ts,ptr+6);
			} else if (strncmp(ptr,"UNDEL",5)==0) {
				status = do_undel(filename,lv,ts,ptr+5);
			} else if (strncmp(ptr,"UNLOCK",6)==0) {
				status = do_unlock(filename,lv,ts,ptr+6);
			} else {
				printf("%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		case 'W':
			if (strncmp(ptr,"WRITE",5)==0) {
				status = do_write(filename,lv,ts,ptr+5);
			} else {
				printf("%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
			}
			break;
		default:
			printf("%s:%"PRIu64": unknown entry '%s'\n",filename,lv,ptr);
	}
	if (status>STATUS_OK) {
		printf("%s:%"PRIu64": error: %"PRIu8" (%s)\n",filename,lv,status,errormsgs[status]);
	}
	return status;
}

static uint64_t v=0,lastv=0;
static const char *lastfn;
static uint8_t vlevel;

int restore(const char *filename,uint64_t lv,char *ptr) {
	int status;
	if (lastv==0 || v==0) {
		v = fs_getversion();
		lastv = lv-1;
		lastfn = "(no file)";
	}
	if (vlevel>1) {
		printf("filename: %s ; current meta version: %"PRIu64" ; previous changeid: %"PRIu64" ; current changeid: %"PRIu64" ; change data%s",filename,v,lastv,lv,ptr);
	}
	if (lv<lastv) {
		printf("merge error - possibly corrupted input file - ignore entry (filename: %s)\n",filename);
		return 0;
	} else if (lv>=v) {
		if (lv==lastv) {
			if (vlevel>1) {
				printf("duplicated entry: %"PRIu64" (previous file: %s, current file: %s)\n",lv,lastfn,filename);
			}
		} else if (lv>lastv+1) {
			printf("hole in change files (entries from %s:%"PRIu64" to %s:%"PRIu64" are missing) - add more files\n",lastfn,lastv+1,filename,lv-1);
			return -2;
		} else {
			if (vlevel>0) {
				printf("%s: change%s",filename,ptr);
			}
			status = restore_line(filename,lv,ptr);
			if (status<0) { // parse error - just ignore this line
				return 0;
			}
			if (status>0) { // other errors - stop processing data
				return -1;
			}
			v = fs_getversion();
			if (lv+1!=v) {
				printf("%s:%"PRIu64": version mismatch\n",filename,lv);
				return -1;
			}
		}
	}
	lastv = lv;
	lastfn = filename;
	return 0;
}

void restore_setverblevel(uint8_t _vlevel) {
	vlevel = _vlevel;
}
