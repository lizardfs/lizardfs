/*
   Copyright 2008 Gemius SA.

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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/poll.h>
#include <syslog.h>
#include <time.h>
#include <pthread.h>

#include "config.h"
#include "MFSCommunication.h"
#include "sockets.h"
#include "datapack.h"

typedef struct _threc {
	pthread_t thid;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	uint8_t *buff;
	uint32_t buffsize;
	uint8_t sent;
	uint8_t status;
	uint8_t release;	// cond variable
	uint32_t size;
	uint32_t cmd;
	uint32_t packetid;
	struct _threc *next;
} threc;

typedef struct _aquired_file {
	uint32_t inode;
	uint32_t cnt;
	struct _aquired_file *next;
} aquired_file;


#define DEFAULT_BUFFSIZE 10000
#define RECEIVE_TIMEOUT 10
#define RETRIES 30

static threc *threchead=NULL;

static aquired_file *afhead=NULL;

static int fd;
static int disconnect;
static time_t lastwrite;

static pthread_t rpthid,npthid;
static pthread_mutex_t fdlock,reclock,aflock;

static uint32_t cuid;
static char *ip;
static char *port;

/*
void fs_lock_acnt(void) {
	pthread_mutex_lock(&aflock);
}

void fs_unlock_acnt(void) {
	pthread_mutex_unlock(&aflock);
}

uint32_t fs_get_acnt(uint32_t inode) {
	aquired_file *afptr;
	for (afptr=afhead ; afptr ; afptr=afptr->next) {
		if (afptr->inode==inode) {
			return (afptr->cnt);
		}
	}
	return 0;
}
*/

void fs_inc_acnt(uint32_t inode) {
	aquired_file *afptr,**afpptr;
	pthread_mutex_lock(&aflock);
	afpptr = &afhead;
	while ((afptr=*afpptr)) {
		if (afptr->inode==inode) {
			afptr->cnt++;
			pthread_mutex_unlock(&aflock);
			return;
		}
		if (afptr->inode>inode) {
			break;
		}
		afpptr = &(afptr->next);
	}
	afptr = (aquired_file*)malloc(sizeof(aquired_file));
	afptr->inode = inode;
	afptr->cnt = 1;
	afptr->next = *afpptr;
	*afpptr = afptr;
	pthread_mutex_unlock(&aflock);
}

void fs_dec_acnt(uint32_t inode) {
	aquired_file *afptr,**afpptr;
	pthread_mutex_lock(&aflock);
	afpptr = &afhead;
	while ((afptr=*afpptr)) {
		if (afptr->inode == inode) {
			if (afptr->cnt<=1) {
				*afpptr = afptr->next;
				free(afptr);
			} else {
				afptr->cnt--;
			}
			pthread_mutex_unlock(&aflock);
			return;
		}
		afpptr = &(afptr->next);
	}
	pthread_mutex_unlock(&aflock);
}

threc* fs_get_my_threc() {
	pthread_t mythid = pthread_self();
	threc *rec;
	pthread_mutex_lock(&reclock);
	for (rec = threchead ; rec ; rec=rec->next) {
		if (pthread_equal(rec->thid,mythid)) {
			pthread_mutex_unlock(&reclock);
			return rec;
		}
	}
	rec = malloc(sizeof(threc));
	rec->thid = mythid;
	if (threchead==NULL) {
		rec->packetid = 1;
	} else {
		rec->packetid = threchead->packetid+1;
	}
	pthread_mutex_init(&(rec->mutex),NULL);
	pthread_cond_init(&(rec->cond),NULL);
	rec->buff = malloc(DEFAULT_BUFFSIZE);
	if (rec->buff==NULL) {
		free(rec);
		pthread_mutex_unlock(&reclock);
		return NULL;
	}
	rec->buffsize = DEFAULT_BUFFSIZE;
	rec->sent = 0;
	rec->status = 0;
	rec->release = 0;
	rec->cmd = 0;
	rec->size = 0;
	rec->next = threchead;
	//syslog(LOG_NOTICE,"mastercomm: create new threc (%d)",rec->packetid);
	threchead = rec;
	pthread_mutex_unlock(&reclock);
	return rec;
}

threc* fs_get_threc_by_id(uint32_t packetid) {
	threc *rec;
	pthread_mutex_lock(&reclock);
	for (rec = threchead ; rec ; rec=rec->next) {
		if (rec->packetid==packetid) {
			pthread_mutex_unlock(&reclock);
			return rec;
		}
	}
	pthread_mutex_unlock(&reclock);
	return NULL;
}

void fs_buffer_init(threc *rec,uint32_t size) {
	if (size>DEFAULT_BUFFSIZE) {
		rec->buff = realloc(rec->buff,size);
		rec->buffsize = size;
	} else if (rec->buffsize>DEFAULT_BUFFSIZE) {
		rec->buff = realloc(rec->buff,DEFAULT_BUFFSIZE);
		rec->buffsize = DEFAULT_BUFFSIZE;
	}
}

uint8_t* fs_createpacket(threc *rec,uint32_t cmd,uint32_t size) {
	uint8_t *ptr;
	uint32_t hdrsize = size+4;
	fs_buffer_init(rec,size+12);
	if (rec->buff==NULL) {
		return NULL;
	}
	ptr = rec->buff;
	PUT32BIT(cmd,ptr);
	PUT32BIT(hdrsize,ptr);
	PUT32BIT(rec->packetid,ptr);
	rec->size = size+12;
	return rec->buff+12;
}

uint8_t* fs_sendandreceive(threc *rec,uint32_t command_info,uint32_t *info_length) {
	uint32_t cnt;
	uint32_t size = rec->size;

	for (cnt=0 ; cnt<RETRIES ; cnt++) {
		pthread_mutex_lock(&fdlock);
		if (fd==-1) {
			pthread_mutex_unlock(&fdlock);
			sleep(1);
			continue;
		}
		//syslog(LOG_NOTICE,"threc(%d) - sending ...",rec->packetid);
		rec->release=0;
		if (tcptowrite(fd,rec->buff,size,1000)!=(int32_t)(size)) {
			syslog(LOG_WARNING,"tcp send error: %m");
			disconnect = 1;
			pthread_mutex_unlock(&fdlock);
			sleep(1);
			continue;
		}
		rec->sent = 1;
		lastwrite = time(NULL);
		pthread_mutex_unlock(&fdlock);
		// syslog(LOG_NOTICE,"master: lock: %u",rec->packetid);
		pthread_mutex_lock(&(rec->mutex));
		while (rec->release==0) { pthread_cond_wait(&(rec->cond),&(rec->mutex)); }
		pthread_mutex_unlock(&(rec->mutex));
		// syslog(LOG_NOTICE,"master: unlocked: %u",rec->packetid);
		// syslog(LOG_NOTICE,"master: command_info: %u ; reccmd: %u",command_info,rec->cmd);
		if (rec->status!=0) {
			sleep(1);
			continue;
		}
		if (rec->cmd!=command_info) {
			pthread_mutex_lock(&fdlock);
			disconnect = 1;
			pthread_mutex_unlock(&fdlock);
			sleep(1);
			continue;
		}
		//syslog(LOG_NOTICE,"threc(%d) - received",rec->packetid);
		*info_length = rec->size;
		return rec->buff+size;
	}
	return NULL;
}

int fs_direct_connect() {
	int rfd;
	rfd = tcpsocket();
	if (tcpconnect(rfd,ip,port)<0) {
		tcpclose(rfd);
		return -1;
	}
	return rfd;
}

void fs_direct_close(int rfd) {
	tcpclose(rfd);
}

int fs_direct_write(int rfd,const uint8_t *buff,uint32_t size) {
	return tcptowrite(rfd,buff,size,60000);
}

int fs_direct_read(int rfd,uint8_t *buff,uint32_t size) {
	return tcptoread(rfd,buff,size,60000);
}

void fs_connect() {
	uint32_t i,ver;
	uint8_t *ptr,regbuff[16+64];

	fd = tcpsocket();
//	if (tcpnodelay(fd)<0) {
//		syslog(LOG_WARNING,"can't set TCP_NODELAY: %m");
//	}
	if (tcpconnect(fd,ip,port)<0) {
		syslog(LOG_WARNING,"can't connect to master (\"%s\":\"%s\")",ip,port);
		tcpclose(fd);
		fd=-1;
		return;
	}
	ptr = regbuff;
	PUT32BIT(CUTOMA_FUSE_REGISTER,ptr);
	PUT32BIT(72,ptr);
	memcpy(ptr,FUSE_REGISTER_BLOB_DNAMES,64);
	ptr+=64;
	PUT32BIT(cuid,ptr);
	ver = VERSMAJ*0x10000+VERSMID*0x100+VERSMIN;
	PUT32BIT(ver,ptr);
	if (tcptowrite(fd,regbuff,16+64,1000)!=16+64) {
		syslog(LOG_WARNING,"master: register error (write: %m)");
		tcpclose(fd);
		fd=-1;
		return;
	}
	if (tcptoread(fd,regbuff,8,1000)!=8) {
		syslog(LOG_WARNING,"master: register error (read header: %m)");
		tcpclose(fd);
		fd=-1;
		return;
	}
	ptr = regbuff;
	GET32BIT(i,ptr);
	if (i!=MATOCU_FUSE_REGISTER) {
		syslog(LOG_WARNING,"master: register error (bad answer: %u)",i);
		tcpclose(fd);
		fd=-1;
		return;
	}
	GET32BIT(i,ptr);
	if (i!=1 && i!=4) {
		syslog(LOG_WARNING,"master: register error (bad length: %u)",i);
		tcpclose(fd);
		fd=-1;
		return;
	}
	if (tcptoread(fd,regbuff,i,1000)!=(int32_t)i) {
		syslog(LOG_WARNING,"master: register error (read data: %m)");
		tcpclose(fd);
		fd=-1;
		return;
	}
	ptr = regbuff;
	if (i==1 && ptr[0]!=0) {
		syslog(LOG_WARNING,"master: register status: %u",ptr[0]);
		tcpclose(fd);
		fd=-1;
		return;
	}
	if (i==4) {
		GET32BIT(cuid,ptr);
	}
	lastwrite=time(NULL);
	syslog(LOG_NOTICE,"registered to master");
}

void* fs_nop_thread(void *arg) {
	uint8_t *ptr,hdr[12],*inodespacket;
	int32_t inodesleng;
	aquired_file *afptr;
	int now;
	int lastinodeswrite=0;
	(void)arg;
	for (;;) {
		now = time(NULL);
		pthread_mutex_lock(&fdlock);
		if (disconnect==0 && fd>=0) {
			if (lastwrite+2<now) {	// NOP
				ptr = hdr;
				PUT32BIT(ANTOAN_NOP,ptr);
				PUT32BIT(4,ptr);
				PUT32BIT(0,ptr);
				if (tcptowrite(fd,hdr,12,1000)!=12) {
					disconnect=1;
				}
				lastwrite=now;
			}
			if (lastinodeswrite+60<now) {	// RESERVED INODES
				pthread_mutex_lock(&aflock);
				inodesleng=8;
				for (afptr=afhead ; afptr ; afptr=afptr->next) {
					//syslog(LOG_NOTICE,"reserved inode: %u",afptr->inode);
					inodesleng+=4;
				}
				inodespacket = malloc(inodesleng);
				ptr = inodespacket;
				PUT32BIT(CUTOMA_FUSE_RESERVED_INODES,ptr);
				PUT32BIT(inodesleng-8,ptr);
				for (afptr=afhead ; afptr ; afptr=afptr->next) {
					PUT32BIT(afptr->inode,ptr);
				}
				if (tcptowrite(fd,inodespacket,inodesleng,1000)!=inodesleng) {
					disconnect=1;
				}
				free(inodespacket);
				pthread_mutex_unlock(&aflock);
				lastinodeswrite=now;
			}
		}
		pthread_mutex_unlock(&fdlock);
		sleep(1);
	}
}

void* fs_receive_thread(void *arg) {
	uint8_t *ptr,hdr[12];
	threc *rec;
	uint32_t cmd,size,packetid;
	int r;

	(void)arg;
	for (;;) {
		pthread_mutex_lock(&fdlock);
		if (disconnect) {
			tcpclose(fd);
			fd=-1;
			disconnect=0;
			// send to any threc status error and unlock them
			pthread_mutex_lock(&reclock);
			for (rec=threchead ; rec ; rec=rec->next) {
				if (rec->sent) {
					rec->status = 1;
					pthread_mutex_lock(&(rec->mutex));
					rec->release = 1;
					pthread_mutex_unlock(&(rec->mutex));
					pthread_cond_signal(&(rec->cond));
				}
			}
			pthread_mutex_unlock(&reclock);
		}
		if (fd==-1) {
			fs_connect();
		}
		if (fd==-1) {
			pthread_mutex_unlock(&fdlock);
			sleep(2);	// reconnect every 2 seconds
			continue;
		}
		pthread_mutex_unlock(&fdlock);
		r = tcptoread(fd,hdr,12,RECEIVE_TIMEOUT*1000);	// read timeout - 4 seconds
		// syslog(LOG_NOTICE,"master: header size: %u",r);
		if (r==0) {
			syslog(LOG_WARNING,"master: connection lost (1)");
			disconnect=1;
			continue;
		}
		if (r!=12) {
			syslog(LOG_WARNING,"master: tcp recv error: %m (1)");
			disconnect=1;
			continue;
		}
		
		ptr = hdr;
		GET32BIT(cmd,ptr);
		GET32BIT(size,ptr);
		GET32BIT(packetid,ptr);
		if (cmd==ANTOAN_NOP && size==4) {
			// syslog(LOG_NOTICE,"master: got nop");
			continue;
		}
		if (size<4) {
			syslog(LOG_WARNING,"master: packet too small");
			disconnect=1;
			continue;
		}
		size-=4;
		rec = fs_get_threc_by_id(packetid);
		if (rec==NULL) {
			syslog(LOG_WARNING,"master: got unexpected queryid");
			disconnect=1;
			continue;
		}
		fs_buffer_init(rec,rec->size+size);
		if (rec->buff==NULL) {
			disconnect=1;
			continue;
		}
		// syslog(LOG_NOTICE,"master: expected data size: %u",size);
		if (size>0) {
			r = tcptoread(fd,rec->buff+rec->size,size,1000);
			// syslog(LOG_NOTICE,"master: data size: %u",r);
			if (r==0) {
				syslog(LOG_WARNING,"master: connection lost (2)");
				disconnect=1;
				continue;
			}
			if (r!=(int32_t)(size)) {
				syslog(LOG_WARNING,"master: tcp recv error: %m (2)");
				disconnect=1;
				continue;
			}
		}
		rec->sent=0;
		rec->status=0;
		rec->size = size;
		rec->cmd = cmd;
		// syslog(LOG_NOTICE,"master: unlock: %u",rec->packetid);
		pthread_mutex_lock(&(rec->mutex));
		rec->release = 1;
		pthread_mutex_unlock(&(rec->mutex));
		pthread_cond_signal(&(rec->cond));
	}
}

void fs_init(char *_ip,char *_port) {
	ip = strdup(_ip);
	port = strdup(_port);
	fd = -1;
	disconnect = 0;
	cuid = 0;
	pthread_mutex_init(&reclock,NULL);
	pthread_mutex_init(&fdlock,NULL);
	pthread_mutex_init(&aflock,NULL);
	pthread_create(&rpthid,NULL,fs_receive_thread,NULL);
	pthread_create(&npthid,NULL,fs_nop_thread,NULL);
}



void fs_statfs(uint64_t *totalspace,uint64_t *availspace,uint64_t *trashspace,uint64_t *reservedspace,uint32_t *inodes) {
	uint64_t t64;
	uint32_t t32;
	uint8_t *ptr;
	uint32_t i;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_STATFS,0);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_STATFS,&i);
	if (ptr==NULL || i!=36) {
		*totalspace = 0;
		*availspace = 0;
		*trashspace = 0;
		*reservedspace = 0;
		*inodes = 0;
	} else {
		GET64BIT(t64,ptr);
		*totalspace = t64;
		GET64BIT(t64,ptr);
		*availspace = t64;
		GET64BIT(t64,ptr);
		*trashspace = t64;
		GET64BIT(t64,ptr);
		*reservedspace = t64;
		GET32BIT(t32,ptr);
		*inodes = t32;
	}
}

uint8_t fs_access(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t modemask) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_ACCESS,13);
	PUT32BIT(inode,ptr);
	PUT32BIT(uid,ptr);
	PUT32BIT(gid,ptr);
	PUT8BIT(modemask,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_ACCESS,&i);
	if (!ptr || i!=1) {
		ret = ERROR_IO;
	} else {
		ret = ptr[0];
	}
	return ret;
}

uint8_t fs_lookup(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *ptr;
	uint32_t i;
	uint32_t t32;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_LOOKUP,13+nleng);
	PUT32BIT(parent,ptr);
	PUT8BIT(nleng,ptr);
	memcpy(ptr,name,nleng);
	ptr+=nleng;
	PUT32BIT(uid,ptr);
	PUT32BIT(gid,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_LOOKUP,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else if (i!=39) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		GET32BIT(t32,ptr);
		*inode = t32;
		memcpy(attr,ptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_getattr(uint32_t inode,uint8_t attr[35]) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_GETATTR,4);
	PUT32BIT(inode,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_GETATTR,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else if (i!=35) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		memcpy(attr,ptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_setattr(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t setmask,uint16_t attrmode,uint32_t attruid,uint32_t attrgid,uint32_t attratime,uint32_t attrmtime,uint8_t attr[35]) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_SETATTR,31);
	PUT32BIT(inode,ptr);
	PUT32BIT(uid,ptr);
	PUT32BIT(gid,ptr);
	PUT8BIT(setmask,ptr);
	PUT16BIT(attrmode,ptr);
	PUT32BIT(attruid,ptr);
	PUT32BIT(attrgid,ptr);
	PUT32BIT(attratime,ptr);
	PUT32BIT(attrmtime,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_SETATTR,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else if (i!=35) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		memcpy(attr,ptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_truncate(uint32_t inode,uint32_t uid,uint32_t gid,uint64_t attrlength,uint8_t attr[35]) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_TRUNCATE,20);
	PUT32BIT(inode,ptr);
	PUT32BIT(uid,ptr);
	PUT32BIT(gid,ptr);
	PUT64BIT(attrlength,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_TRUNCATE,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else if (i!=35) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		memcpy(attr,ptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_readlink(uint32_t inode,uint8_t **path) {
	uint8_t *ptr;
	uint32_t i;
	uint32_t pleng;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_READLINK,4);
	PUT32BIT(inode,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_READLINK,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else if (i<4) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		GET32BIT(pleng,ptr);
		if (i!=4+pleng || pleng==0 || ptr[pleng-1]!=0) {
			pthread_mutex_lock(&fdlock);
			disconnect = 1;
			pthread_mutex_unlock(&fdlock);
			ret = ERROR_IO;
		} else {
			*path = ptr;
			//*path = malloc(pleng);
			//memcpy(*path,ptr,pleng);
			ret = STATUS_OK;
		}
	}
	return ret;
}

uint8_t fs_symlink(uint32_t parent,uint8_t nleng,const uint8_t *name,const uint8_t *path,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *ptr;
	uint32_t i;
	uint32_t t32;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	t32 = strlen((const char *)path)+1;
	ptr = fs_createpacket(rec,CUTOMA_FUSE_SYMLINK,t32+nleng+17);
	PUT32BIT(parent,ptr);
	PUT8BIT(nleng,ptr);
	memcpy(ptr,name,nleng);
	ptr+=nleng;
	PUT32BIT(t32,ptr);
	memcpy(ptr,path,t32);
	ptr+=t32;
	PUT32BIT(uid,ptr);
	PUT32BIT(gid,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_SYMLINK,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else if (i!=39) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		GET32BIT(t32,ptr);
		*inode = t32;
		memcpy(attr,ptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_mknod(uint32_t parent,uint8_t nleng,const uint8_t *name,uint8_t type,uint16_t mode,uint32_t uid,uint32_t gid,uint32_t rdev,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *ptr;
	uint32_t i;
	uint32_t t32;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_MKNOD,20+nleng);
	PUT32BIT(parent,ptr);
	PUT8BIT(nleng,ptr);
	memcpy(ptr,name,nleng);
	ptr+=nleng;
	PUT8BIT(type,ptr);
	PUT16BIT(mode,ptr);
	PUT32BIT(uid,ptr);
	PUT32BIT(gid,ptr);
	PUT32BIT(rdev,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_MKNOD,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else if (i!=39) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		GET32BIT(t32,ptr);
		*inode = t32;
		memcpy(attr,ptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_mkdir(uint32_t parent,uint8_t nleng,const uint8_t *name,uint16_t mode,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *ptr;
	uint32_t i;
	uint32_t t32;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_MKDIR,15+nleng);
	PUT32BIT(parent,ptr);
	PUT8BIT(nleng,ptr);
	memcpy(ptr,name,nleng);
	ptr+=nleng;
	PUT16BIT(mode,ptr);
	PUT32BIT(uid,ptr);
	PUT32BIT(gid,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_MKDIR,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else if (i!=39) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		GET32BIT(t32,ptr);
		*inode = t32;
		memcpy(attr,ptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_unlink(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_UNLINK,13+nleng);
	PUT32BIT(parent,ptr);
	PUT8BIT(nleng,ptr);
	memcpy(ptr,name,nleng);
	ptr+=nleng;
	PUT32BIT(uid,ptr);
	PUT32BIT(gid,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_UNLINK,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_rmdir(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_RMDIR,13+nleng);
	PUT32BIT(parent,ptr);
	PUT8BIT(nleng,ptr);
	memcpy(ptr,name,nleng);
	ptr+=nleng;
	PUT32BIT(uid,ptr);
	PUT32BIT(gid,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_RMDIR,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_rename(uint32_t parent_src,uint8_t nleng_src,const uint8_t *name_src,uint32_t parent_dst,uint8_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gid) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_RENAME,18+nleng_src+nleng_dst);
	PUT32BIT(parent_src,ptr);
	PUT8BIT(nleng_src,ptr);
	memcpy(ptr,name_src,nleng_src);
	ptr+=nleng_src;
	PUT32BIT(parent_dst,ptr);
	PUT8BIT(nleng_dst,ptr);
	memcpy(ptr,name_dst,nleng_dst);
	ptr+=nleng_dst;
	PUT32BIT(uid,ptr);
	PUT32BIT(gid,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_RENAME,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_link(uint32_t inode_src,uint32_t parent_dst,uint8_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *ptr;
	uint32_t i;
	uint32_t t32;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_LINK,17+nleng_dst);
	PUT32BIT(inode_src,ptr);
	PUT32BIT(parent_dst,ptr);
	PUT8BIT(nleng_dst,ptr);
	memcpy(ptr,name_dst,nleng_dst);
	ptr+=nleng_dst;
	PUT32BIT(uid,ptr);
	PUT32BIT(gid,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_LINK,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else if (i!=39) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		GET32BIT(t32,ptr);
		*inode = t32;
		memcpy(attr,ptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_getdir(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t **dbuff,uint32_t *dbuffsize) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_GETDIR,12);
	PUT32BIT(inode,ptr);
	PUT32BIT(uid,ptr);
	PUT32BIT(gid,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_GETDIR,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else {
		*dbuff = ptr;
		*dbuffsize = i;
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_check(uint32_t inode,uint8_t dbuff[22]) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	uint16_t cbuff[11];
	uint8_t copies;
	uint16_t chunks;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_CHECK,4);
	PUT32BIT(inode,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_CHECK,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else if (i%3!=0) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		for (copies=0 ; copies<11 ; copies++) {
			cbuff[copies]=0;
		}
		while (i>0) {
			GET8BIT(copies,ptr);
			GET16BIT(chunks,ptr);
			if (copies<10) {
				cbuff[copies]+=chunks;
			} else {
				cbuff[10]+=chunks;
			}
			i-=3;
		}
		ptr = dbuff;
		for (copies=0 ; copies<11 ; copies++) {
			chunks = cbuff[copies];
			PUT16BIT(chunks,ptr);
		}
		ret = STATUS_OK;
	}
	return ret;
}

// FUSE - I/O

uint8_t fs_opencheck(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t flags) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_OPEN,13);
	PUT32BIT(inode,ptr);
	PUT32BIT(uid,ptr);
	PUT32BIT(gid,ptr);
	PUT8BIT(flags,ptr);
	fs_inc_acnt(inode);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_OPEN,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	if (ret) {	// release on error
		fs_dec_acnt(inode);
	}
	return ret;
}

void fs_release(uint32_t inode) {
	fs_dec_acnt(inode);
}

// release - decrease aquire cnt - if reach 0 send CUTOMA_FUSE_RELEASE
/*
uint8_t fs_release(uint32_t inode) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	ptr = fs_createpacket(rec,CUTOMA_FUSE_RELEASE,4);
	PUT32BIT(inode,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_RELEASE,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	return ret;
}
*/

uint8_t fs_readchunk(uint32_t inode,uint32_t indx,uint64_t *length,uint64_t *chunkid,uint32_t *version,uint32_t *csip,uint16_t *csport) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	uint64_t t64;
	uint32_t t32;
	uint16_t t16;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_READ_CHUNK,8);
	PUT32BIT(inode,ptr);
	PUT32BIT(indx,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_READ_CHUNK,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else if (i<20 || ((i-20)%6)!=0) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		GET64BIT(t64,ptr);
		*length = t64;
		GET64BIT(t64,ptr);
		*chunkid = t64;
		GET32BIT(t32,ptr);
		*version = t32;
		if (i==20) {
			*csip = 0;
			*csport = 0;
		} else {
			GET32BIT(t32,ptr);
			*csip = t32;
			GET16BIT(t16,ptr);
			*csport = t16;
		}
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_writechunk(uint32_t inode,uint32_t indx,uint64_t *length,uint64_t *chunkid,uint32_t *version,uint32_t *csip,uint16_t *csport,uint8_t **chain,uint32_t *chainsize) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	uint64_t t64;
	uint32_t t32;
	uint16_t t16;
	threc *rec = fs_get_my_threc();
	*chain=NULL;
	*chainsize = 0;
	ptr = fs_createpacket(rec,CUTOMA_FUSE_WRITE_CHUNK,8);
	PUT32BIT(inode,ptr);
	PUT32BIT(indx,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_WRITE_CHUNK,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else if (i<20 || ((i-20)%6)!=0) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		GET64BIT(t64,ptr);
		*length = t64;
		GET64BIT(t64,ptr);
		*chunkid = t64;
		GET32BIT(t32,ptr);
		*version = t32;
		if (i==20) {
			*csip = 0;
			*csport = 0;
			*chain = NULL;
			*chainsize = 0;
		} else {
			GET32BIT(t32,ptr);
			*csip = t32;
			GET16BIT(t16,ptr);
			*csport = t16;
			if (i>26) {
				*chain = ptr;
				//*chain = malloc(i-26);
				*chainsize = i-26;
				//memcpy(*chain,ptr,i-26);
			} else {
				*chain = NULL;
				*chainsize = 0;
			}
		}
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_writeend(uint64_t chunkid, uint32_t inode, uint64_t length) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_WRITE_CHUNK_END,20);
	PUT64BIT(chunkid,ptr);
	PUT32BIT(inode,ptr);
	PUT64BIT(length,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_WRITE_CHUNK_END,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	return ret;
}


// FUSE - META


uint8_t fs_getreserved(uint8_t **dbuff,uint32_t *dbuffsize) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_GETRESERVED,0);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_GETRESERVED,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else {
		*dbuff = ptr;
		*dbuffsize = i;
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_gettrash(uint8_t **dbuff,uint32_t *dbuffsize) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_GETTRASH,0);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_GETTRASH,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else {
		*dbuff = ptr;
		*dbuffsize = i;
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_getdetachedattr(uint32_t inode,uint8_t attr[35]) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_GETDETACHEDATTR,4);
	PUT32BIT(inode,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_GETDETACHEDATTR,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else if (i!=35) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		memcpy(attr,ptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_gettrashpath(uint32_t inode,uint8_t **path) {
	uint8_t *ptr;
	uint32_t i;
	uint32_t pleng;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_GETTRASHPATH,4);
	PUT32BIT(inode,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_GETTRASHPATH,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else if (i<4) {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	} else {
		GET32BIT(pleng,ptr);
		if (i!=4+pleng || pleng==0 || ptr[pleng-1]!=0) {
			pthread_mutex_lock(&fdlock);
			disconnect = 1;
			pthread_mutex_unlock(&fdlock);
			ret = ERROR_IO;
		} else {
			*path = ptr;
			ret = STATUS_OK;
		}
	}
	return ret;
}

uint8_t fs_settrashpath(uint32_t inode,uint8_t *path) {
	uint8_t *ptr;
	uint32_t i;
	uint32_t t32;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	t32 = strlen((const char *)path)+1;
	ptr = fs_createpacket(rec,CUTOMA_FUSE_SETTRASHPATH,t32+8);
	PUT32BIT(inode,ptr);
	PUT32BIT(t32,ptr);
	memcpy(ptr,path,t32);
//	ptr+=t32;
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_SETTRASHPATH,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_undel(uint32_t inode) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_UNDEL,4);
	PUT32BIT(inode,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_UNDEL,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_purge(uint32_t inode) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_PURGE,4);
	PUT32BIT(inode,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_PURGE,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_append(uint32_t inode,uint32_t ainode,uint32_t uid,uint32_t gid) {
	uint8_t *ptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	ptr = fs_createpacket(rec,CUTOMA_FUSE_APPEND,16);
	PUT32BIT(inode,ptr);
	PUT32BIT(ainode,ptr);
	PUT32BIT(uid,ptr);
	PUT32BIT(gid,ptr);
	ptr = fs_sendandreceive(rec,MATOCU_FUSE_APPEND,&i);
	if (ptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = ptr[0];
	} else {
		pthread_mutex_lock(&fdlock);
		disconnect = 1;
		pthread_mutex_unlock(&fdlock);
		ret = ERROR_IO;
	}
	return ret;
}
