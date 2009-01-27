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

#include "config.h"

#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <libgen.h>
#include <fcntl.h>
#include <inttypes.h>
#include <poll.h>

#include "datapack.h"
#include "MFSCommunication.h"

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)
const char id1[]="@(#) version: " STR(VERSMAJ) "." STR(VERSMID) "." STR(VERSMIN) ", written by Jakub Kruszona-Zawadzki";
const char id2[]="@(#) Copyright 2005 by Gemius S.A.";

#define INODE_VALUE_MASK 0x1FFFFFFF
#define INODE_TYPE_MASK 0x60000000
#define INODE_TYPE_TRASH 0x20000000
#define INODE_TYPE_RESERVED 0x40000000
#define INODE_TYPE_SPECIAL 0x00000000

const char* errtab[]={ERROR_STRINGS};

/* bsd_humanize_number */
#define HN_DECIMAL		0x01
#define HN_NOSPACE		0x02
#define HN_B			0x04
#define HN_DIVISOR_1000		0x08

#define HN_GETSCALE		0x10
#define HN_AUTOSCALE		0x20

int bsd_humanize_number(char *buf, size_t len, int64_t bytes, const char *suffix, int scale, int flags) {
	const char *prefixes, *sep;
	int b, i, r, maxscale, s1, s2, sign;
	int64_t divisor, max;
	size_t baselen;

	if (flags & HN_DIVISOR_1000) {
		/* SI for decimal multiplies */
		divisor = 1000;
		if (flags & HN_B) {
			prefixes = "B\0k\0M\0G\0T\0P\0E";
		} else {
			prefixes = "\0\0k\0M\0G\0T\0P\0E";
		}
	} else {
		/*
		 * binary multiplies
		 * XXX IEC 60027-2 recommends Ki, Mi, Gi...
		 */
		divisor = 1024;
		if (flags & HN_B) {
			prefixes = "B\0K\0M\0G\0T\0P\0E";
		} else {
			prefixes = "\0\0K\0M\0G\0T\0P\0E";
		}
	}

#define	SCALE2PREFIX(scale)	(&prefixes[(scale) << 1])
	maxscale = 7;

	if (scale<0 || (scale >= maxscale && (scale & (HN_AUTOSCALE | HN_GETSCALE)) == 0)) {
		return -1;
	}

	if (buf == NULL || suffix == NULL) {
		return -1;
	}

	if (len > 0) {
		buf[0] = '\0';
	}
	if (bytes < 0) {
		sign = -1;
		bytes *= -100;
		baselen = 3;		/* sign, digit, prefix */
	} else {
		sign = 1;
		bytes *= 100;
		baselen = 2;		/* digit, prefix */
	}
	if (flags & HN_NOSPACE) {
		sep = "";
	} else {
		sep = " ";
		baselen++;
	}
	baselen += strlen(suffix);

	/* Check if enough room for `x y' + suffix + `\0' */
	if (len < baselen + 1) {
		return -1;
	}

	if (scale & (HN_AUTOSCALE | HN_GETSCALE)) {
		/* See if there is additional columns can be used. */
		for (max = 100, i = len - baselen; i-- > 0;) {
			max *= 10;
		}
		for (i = 0; bytes >= max && i < maxscale; i++) {
			bytes /= divisor;
		}
		if (scale & HN_GETSCALE) {
			return i;
		}
	} else {
		for (i = 0; i < scale && i < maxscale; i++) {
			bytes /= divisor;
		}
	}

	/* If a value <= 9.9 after rounding and ... */
	if (bytes < 995 && i > 0 && flags & HN_DECIMAL) {
		/* baselen + \0 + .N */
		if (len < baselen + 1 + 2) {
			return -1;
		}
		b = ((int)bytes + 5) / 10;
		s1 = b / 10;
		s2 = b % 10;
		r = snprintf(buf, len, "%d.%d%s%s%s", sign * s1, s2, sep, SCALE2PREFIX(i), suffix);
	} else {
		r = snprintf(buf, len, "%lld%s%s%s", (long long)(sign * ((bytes + 50) / 100)), sep, SCALE2PREFIX(i), suffix);
	}
	return r;
}

int32_t socket_read(int sock,void *buff,uint32_t leng) {
	uint32_t rcvd=0;
	int i;
	while (rcvd<leng) {
		i = read(sock,((uint8_t*)buff)+rcvd,leng-rcvd);
		if (i<=0) return i;
		rcvd+=i;
	}
	return rcvd;
}

int32_t socket_write(int sock,void *buff,uint32_t leng) {
	uint32_t sent=0;
	int i;
	while (sent<leng) {
		i = write(sock,((uint8_t*)buff)+sent,leng-sent);
		if (i<=0) return i;
		sent+=i;
	}
	return sent;
}

int master_register(int rfd) {
	uint32_t i;
	uint8_t *ptr,regbuff[8+64];

	ptr = regbuff;
	PUT32BIT(CUTOMA_FUSE_REGISTER,ptr);
	PUT32BIT(64,ptr);
	memcpy(ptr,FUSE_REGISTER_BLOB_NOPS,64);
	if (socket_write(rfd,regbuff,8+64)!=8+64) {
		printf("register to master: send error\n");
		return -1;
	}
	if (socket_read(rfd,regbuff,9)!=9) {
		printf("register to master: receive error\n");
		return -1;
	}
	ptr = regbuff;
	GET32BIT(i,ptr);
	if (i!=MATOCU_FUSE_REGISTER) {
		printf("register to master: wrong answer (type)\n");
		return -1;
	}
	GET32BIT(i,ptr);
	if (i!=1) {
		printf("register to master: wrong answer (length)\n");
		return -1;
	}
	if (*ptr) {
		printf("register to master: %s\n",errtab[*ptr]);
		return -1;
	}
	return 0;
}

int open_master_conn(const char *name,uint32_t *inode) {
	char rpath[PATH_MAX],*p;
	struct stat stb;
	int sd;
	if (realpath(name,rpath)==NULL) {
		printf("%s: realpath error\n",name);
		return -1;
	}
	p = rpath;
	if (lstat(p,&stb)!=0) {
		printf("%s: (%s) lstat error\n",name,p);
		return -1;
	}
	*inode = stb.st_ino;
	for(;;) {
		if (stb.st_ino==1) {	// found fuse root
			p = strcat(p,"/.master");
			if (lstat(p,&stb)==0) {
				if ((stb.st_ino==0x7FFFFFFF || stb.st_ino==0x7FFFFFFE) && stb.st_nlink==1 && stb.st_uid==0 && stb.st_gid==0) {
					if (stb.st_ino==0x7FFFFFFE) {	// meta master
						if (((*inode)&INODE_TYPE_MASK)!=INODE_TYPE_TRASH && ((*inode)&INODE_TYPE_MASK)!=INODE_TYPE_RESERVED) {
							printf("%s: only files in 'trash' and 'reserved' are usable in mfsmeta\n",name);
							return -1;
						}
						(*inode)&=INODE_VALUE_MASK;
					}
					sd = open(p,O_RDWR);
					if (master_register(sd)<0) {
						printf("%s: can't register to master\n",name);
						return -1;
					}
					return sd;
				}
			}
			printf("%s: not MFS object\n",name);
			return -1;
		}
		if (p[0]!='/' || p[1]=='\0') {
			printf("%s: not MFS object\n",name);
			return -1;
		}
		p = dirname(p);
		if (lstat(p,&stb)!=0) {
			printf("%s: (%s) lstat error\n",name,p);
			return -1;
		}
	}
	return -1;
}

int open_two_files_master_conn(const char *fname,const char *sname,uint32_t *finode,uint32_t *sinode) {
	char frpath[PATH_MAX];
	char srpath[PATH_MAX];
	int i;
	char *p;
	struct stat stb;
	int sd;
	if (realpath(fname,frpath)==NULL) {
		printf("%s: realpath error\n",fname);
		return -1;
	}
	if (realpath(sname,srpath)==NULL) {
		printf("%s: realpath error\n",sname);
		return -1;
	}
	if (lstat(frpath,&stb)!=0) {
		printf("%s: (%s) lstat error\n",fname,frpath);
		return -1;
	}
	*finode = stb.st_ino;
	if (lstat(srpath,&stb)!=0) {
		printf("%s: (%s) lstat error\n",sname,srpath);
		return -1;
	}
	*sinode = stb.st_ino;

	for (i=0 ; i<PATH_MAX && frpath[i]==srpath[i] ; i++) {}
	frpath[i]='\0';
	p = dirname(frpath);
	if (lstat(p,&stb)!=0) {
		printf("%s: lstat error\n",p);
		return -1;
	}
	for(;;) {
		if (stb.st_ino==1) {	// found fuse root
			p = strcat(p,"/.master");
			if (lstat(p,&stb)==0) {
				if ((stb.st_ino==0x7FFFFFFF || stb.st_ino==0x7FFFFFFE) && stb.st_nlink==1 && stb.st_uid==0 && stb.st_gid==0) {
					if (stb.st_ino==0x7FFFFFFE) {	// meta master
						if ((((*finode)&INODE_TYPE_MASK)!=INODE_TYPE_TRASH && ((*finode)&INODE_TYPE_MASK)!=INODE_TYPE_RESERVED) \
						 || (((*sinode)&INODE_TYPE_MASK)!=INODE_TYPE_TRASH && ((*sinode)&INODE_TYPE_MASK)!=INODE_TYPE_RESERVED)) {
							printf("%s,%s: only files in 'trash' and 'reserved' are usable in mfsmeta\n",fname,sname);
							return -1;
						}
						(*finode)&=INODE_VALUE_MASK;
						(*sinode)&=INODE_VALUE_MASK;
					}
					sd = open(p,O_RDWR);
					if (master_register(sd)<0) {
						printf("%s,%s: can't register to master\n",fname,sname);
						return -1;
					}
					return sd;
				}
			}
			printf("%s,%s: not same MFS objects\n",fname,sname);
			return -1;
		}
		if (p[0]!='/' || p[1]=='\0') {
			printf("%s,%s: not same MFS objects\n",fname,sname);
			return -1;
		}
		p = dirname(p);
		if (lstat(p,&stb)!=0) {
			printf("%s: lstat error\n",p);
			return -1;
		}
	}
	return -1;
}

int check_file(const char* fname) {
	uint8_t reqbuff[16],*ptr,*buff;
	uint32_t cmd,leng,inode;
	uint8_t copies;
	uint16_t chunks;
	int fd;
	fd = open_master_conn(fname,&inode);
	if (fd<0) {
		return -1;
	}
	ptr = reqbuff;
	PUT32BIT(CUTOMA_FUSE_CHECK,ptr);
	PUT32BIT(8,ptr);
	PUT32BIT(0,ptr);
	PUT32BIT(inode,ptr);
	if (socket_write(fd,reqbuff,16)!=16) {
		printf("%s: master query: send error\n",fname);
		close(fd);
		return -1;
	}
	if (socket_read(fd,reqbuff,8)!=8) {
		printf("%s: master query: receive error\n",fname);
		close(fd);
		return -1;
	}
	ptr = reqbuff;
	GET32BIT(cmd,ptr);
	GET32BIT(leng,ptr);
	if (cmd!=MATOCU_FUSE_CHECK) {
		printf("%s: master query: wrong answer (type)\n",fname);
		close(fd);
		return -1;
	}
	buff = malloc(leng);
	if (socket_read(fd,buff,leng)!=(int32_t)leng) {
		printf("%s: master query: receive error\n",fname);
		free(buff);
		close(fd);
		return -1;
	}
	close(fd);
	ptr = buff;
	GET32BIT(cmd,ptr);	// queryid
	if (cmd!=0) {
		printf("%s: master query: wrong answer (queryid)\n",fname);
		free(buff);
		return -1;
	}
	leng-=4;
	if (leng==1) {
		printf("%s: %s\n",fname,errtab[*ptr]);
		free(buff);
		return -1;
	} else if (leng%3!=0) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		free(buff);
		return -1;
	}
	printf("%s:\n",fname);
	for (cmd=0 ; cmd<leng ; cmd+=3) {
		GET8BIT(copies,ptr);
		GET16BIT(chunks,ptr);
		printf("%"PRIu8" copies: %"PRIu16" chunks\n",copies,chunks);
	}
	free(buff);
	return 0;
}

int get_goal(const char *fname,uint8_t mode) {
	uint8_t reqbuff[17],*ptr,*buff;
	uint32_t cmd,leng,inode;
	uint8_t fn,dn,i;
	uint8_t goal;
	uint32_t cnt;
	char hbuf[5];
	int fd;
	fd = open_master_conn(fname,&inode);
	if (fd<0) {
		return -1;
	}
	ptr = reqbuff;
	PUT32BIT(CUTOMA_FUSE_GETGOAL,ptr);
	PUT32BIT(9,ptr);
	PUT32BIT(0,ptr);
	PUT32BIT(inode,ptr);
	PUT8BIT(mode,ptr);
	if (socket_write(fd,reqbuff,17)!=17) {
		printf("%s: master query: send error\n",fname);
		close(fd);
		return -1;
	}
	if (socket_read(fd,reqbuff,8)!=8) {
		printf("%s: master query: receive error\n",fname);
		close(fd);
		return -1;
	}
	ptr = reqbuff;
	GET32BIT(cmd,ptr);
	GET32BIT(leng,ptr);
	if (cmd!=MATOCU_FUSE_GETGOAL) {
		printf("%s: master query: wrong answer (type)\n",fname);
		close(fd);
		return -1;
	}
	buff = malloc(leng);
	if (socket_read(fd,buff,leng)!=(int32_t)leng) {
		printf("%s: master query: receive error\n",fname);
		free(buff);
		close(fd);
		return -1;
	}
	close(fd);
	ptr = buff;
	GET32BIT(cmd,ptr);	// queryid
	if (cmd!=0) {
		printf("%s: master query: wrong answer (queryid)\n",fname);
		free(buff);
		return -1;
	}
	leng-=4;
	if (leng==1) {
		printf("%s: %s\n",fname,errtab[*ptr]);
		free(buff);
		return -1;
	} else if (leng%5!=2) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		free(buff);
		return -1;
	} else if (mode==GMODE_NORMAL && leng!=7) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		free(buff);
		return -1;
	}
	if (mode==GMODE_NORMAL) {
		GET8BIT(fn,ptr);
		GET8BIT(dn,ptr);
		GET8BIT(goal,ptr);
		GET32BIT(cnt,ptr);
		if ((fn!=0 || dn!=1) && (fn!=1 || dn!=0)) {
			printf("%s: master query: wrong answer (fn,dn)\n",fname);
			free(buff);
			return -1;
		}
		if (cnt!=1) {
			printf("%s: master query: wrong answer (cnt)\n",fname);
			free(buff);
			return -1;
		}
		printf("%s: %"PRIu8"\n",fname,goal);
	} else {
		GET8BIT(fn,ptr);
		GET8BIT(dn,ptr);
		printf("%s:\n",fname);
		for (i=0 ; i<fn ; i++) {
			GET8BIT(goal,ptr);
			GET32BIT(cnt,ptr);
			bsd_humanize_number(hbuf, sizeof(hbuf), cnt, "", HN_AUTOSCALE, HN_NOSPACE | HN_DECIMAL);
			printf(" files with goal        %"PRIu8" : %4s (%"PRIu32")\n",goal,hbuf,cnt);
		}
		for (i=0 ; i<dn ; i++) {
			GET8BIT(goal,ptr);
			GET32BIT(cnt,ptr);
			bsd_humanize_number(hbuf, sizeof(hbuf), cnt, "", HN_AUTOSCALE, HN_NOSPACE | HN_DECIMAL);
			printf(" directories with goal  %"PRIu8" : %4s (%"PRIu32")\n",goal,hbuf,cnt);
		}
	}
	free(buff);
	return 0;
}

int get_trashtime(const char *fname,uint8_t mode) {
	uint8_t reqbuff[17],*ptr,*buff;
	uint32_t cmd,leng,inode;
	uint32_t fn,dn,i;
	uint32_t trashtime;
	uint32_t cnt;
	char hbuf[5];
	int fd;
	fd = open_master_conn(fname,&inode);
	if (fd<0) {
		return -1;
	}
	ptr = reqbuff;
	PUT32BIT(CUTOMA_FUSE_GETTRASHTIME,ptr);
	PUT32BIT(9,ptr);
	PUT32BIT(0,ptr);
	PUT32BIT(inode,ptr);
	PUT8BIT(mode,ptr);
	if (socket_write(fd,reqbuff,17)!=17) {
		printf("%s: master query: send error\n",fname);
		close(fd);
		return -1;
	}
	if (socket_read(fd,reqbuff,8)!=8) {
		printf("%s: master query: receive error\n",fname);
		close(fd);
		return -1;
	}
	ptr = reqbuff;
	GET32BIT(cmd,ptr);
	GET32BIT(leng,ptr);
	if (cmd!=MATOCU_FUSE_GETTRASHTIME) {
		printf("%s: master query: wrong answer (type)\n",fname);
		close(fd);
		return -1;
	}
	buff = malloc(leng);
	if (socket_read(fd,buff,leng)!=(int32_t)leng) {
		printf("%s: master query: receive error\n",fname);
		free(buff);
		close(fd);
		return -1;
	}
	close(fd);
	ptr = buff;
	GET32BIT(cmd,ptr);	// queryid
	if (cmd!=0) {
		printf("%s: master query: wrong answer (queryid)\n",fname);
		free(buff);
		return -1;
	}
	leng-=4;
	if (leng==1) {
		printf("%s: %s\n",fname,errtab[*ptr]);
		free(buff);
		return -1;
	} else if (leng<8 || leng%8!=0) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		free(buff);
		return -1;
	} else if (mode==GMODE_NORMAL && leng!=16) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		free(buff);
		return -1;
	}
	if (mode==GMODE_NORMAL) {
		GET32BIT(fn,ptr);
		GET32BIT(dn,ptr);
		GET32BIT(trashtime,ptr);
		GET32BIT(cnt,ptr);
		if ((fn!=0 || dn!=1) && (fn!=1 || dn!=0)) {
			printf("%s: master query: wrong answer (fn,dn)\n",fname);
			free(buff);
			return -1;
		}
		if (cnt!=1) {
			printf("%s: master query: wrong answer (cnt)\n",fname);
			free(buff);
			return -1;
		}
		printf("%s: %"PRIu32"\n",fname,trashtime);
	} else {
		GET32BIT(fn,ptr);
		GET32BIT(dn,ptr);
		printf("%s:\n",fname);
		for (i=0 ; i<fn ; i++) {
			GET32BIT(trashtime,ptr);
			GET32BIT(cnt,ptr);
			bsd_humanize_number(hbuf, sizeof(hbuf), cnt, "", HN_AUTOSCALE, HN_NOSPACE | HN_DECIMAL);
			printf(" files with trashtime        %10"PRIu32" : %4s (%"PRIu32")\n",trashtime,hbuf,cnt);
		}
		for (i=0 ; i<dn ; i++) {
			GET32BIT(trashtime,ptr);
			GET32BIT(cnt,ptr);
			bsd_humanize_number(hbuf, sizeof(hbuf), cnt, "", HN_AUTOSCALE, HN_NOSPACE | HN_DECIMAL);
			printf(" directories with trashtime  %10"PRIu32" : %4s (%"PRIu32")\n",trashtime,hbuf,cnt);
		}
	}
	free(buff);
	return 0;
}

int set_goal(const char *fname,uint8_t goal,uint8_t mode) {
	uint8_t reqbuff[22],*ptr,*buff;
	uint32_t cmd,leng,inode,uid;
	uint32_t changed,notchanged,notpermitted;
	char hbuf[5];
	int fd;
	fd = open_master_conn(fname,&inode);
	if (fd<0) {
		return -1;
	}
	uid = getuid();
	ptr = reqbuff;
	PUT32BIT(CUTOMA_FUSE_SETGOAL,ptr);
	PUT32BIT(14,ptr);
	PUT32BIT(0,ptr);
	PUT32BIT(inode,ptr);
	PUT32BIT(uid,ptr);
	PUT8BIT(goal,ptr);
	PUT8BIT(mode,ptr);
	if (socket_write(fd,reqbuff,22)!=22) {
		printf("%s: master query: send error\n",fname);
		close(fd);
		return -1;
	}
	if (socket_read(fd,reqbuff,8)!=8) {
		printf("%s: master query: receive error\n",fname);
		close(fd);
		return -1;
	}
	ptr = reqbuff;
	GET32BIT(cmd,ptr);
	GET32BIT(leng,ptr);
	if (cmd!=MATOCU_FUSE_SETGOAL) {
		printf("%s: master query: wrong answer (type)\n",fname);
		close(fd);
		return -1;
	}
	buff = malloc(leng);
	if (socket_read(fd,buff,leng)!=(int32_t)leng) {
		printf("%s: master query: receive error\n",fname);
		free(buff);
		close(fd);
		return -1;
	}
	close(fd);
	ptr = buff;
	GET32BIT(cmd,ptr);	// queryid
	if (cmd!=0) {
		printf("%s: master query: wrong answer (queryid)\n",fname);
		free(buff);
		return -1;
	}
	leng-=4;
	if (leng==1) {
		printf("%s: %s\n",fname,errtab[*ptr]);
		free(buff);
		return -1;
	} else if (leng!=12) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		free(buff);
		return -1;
	}
	GET32BIT(changed,ptr);
	GET32BIT(notchanged,ptr);
	GET32BIT(notpermitted,ptr);
	if ((mode&SMODE_RMASK)==0) {
		if (changed || mode==SMODE_SET) {
			printf("%s: %"PRIu8"\n",fname,goal);
		} else {
			printf("%s: goal not changed\n",fname);
		}
	} else {
		printf("%s:\n",fname);
		bsd_humanize_number(hbuf, sizeof(hbuf), changed, "", HN_AUTOSCALE, HN_NOSPACE | HN_DECIMAL);
		printf(" inodes with goal changed:      %4s (%"PRIu32")\n",hbuf,changed);
		bsd_humanize_number(hbuf, sizeof(hbuf), notchanged, "", HN_AUTOSCALE, HN_NOSPACE | HN_DECIMAL);
		printf(" inodes with goal not changed:  %4s (%"PRIu32")\n",hbuf,notchanged);
		bsd_humanize_number(hbuf, sizeof(hbuf), notpermitted, "", HN_AUTOSCALE, HN_NOSPACE | HN_DECIMAL);
		printf(" inodes with permission denied: %4s (%"PRIu32")\n",hbuf,notpermitted);
	}
	free(buff);
	return 0;
}

int set_trashtime(const char *fname,uint32_t trashtime,uint8_t mode) {
	uint8_t reqbuff[25],*ptr,*buff;
	uint32_t cmd,leng,inode,uid;
	uint32_t changed,notchanged,notpermitted;
	char hbuf[5];
	int fd;
	fd = open_master_conn(fname,&inode);
	if (fd<0) {
		return -1;
	}
	uid = getuid();
	ptr = reqbuff;
	PUT32BIT(CUTOMA_FUSE_SETTRASHTIME,ptr);
	PUT32BIT(17,ptr);
	PUT32BIT(0,ptr);
	PUT32BIT(inode,ptr);
	PUT32BIT(uid,ptr);
	PUT32BIT(trashtime,ptr);
	PUT8BIT(mode,ptr);
	if (socket_write(fd,reqbuff,25)!=25) {
		printf("%s: master query: send error\n",fname);
		close(fd);
		return -1;
	}
	if (socket_read(fd,reqbuff,8)!=8) {
		printf("%s: master query: receive error\n",fname);
		close(fd);
		return -1;
	}
	ptr = reqbuff;
	GET32BIT(cmd,ptr);
	GET32BIT(leng,ptr);
	if (cmd!=MATOCU_FUSE_SETTRASHTIME) {
		printf("%s: master query: wrong answer (type)\n",fname);
		close(fd);
		return -1;
	}
	buff = malloc(leng);
	if (socket_read(fd,buff,leng)!=(int32_t)leng) {
		printf("%s: master query: receive error\n",fname);
		free(buff);
		close(fd);
		return -1;
	}
	close(fd);
	ptr = buff;
	GET32BIT(cmd,ptr);	// queryid
	if (cmd!=0) {
		printf("%s: master query: wrong answer (queryid)\n",fname);
		free(buff);
		return -1;
	}
	leng-=4;
	if (leng==1) {
		printf("%s: %s\n",fname,errtab[*ptr]);
		free(buff);
		return -1;
	} else if (leng!=12) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		free(buff);
		return -1;
	}
	GET32BIT(changed,ptr);
	GET32BIT(notchanged,ptr);
	GET32BIT(notpermitted,ptr);
	if ((mode&SMODE_RMASK)==0) {
		if (changed || mode==SMODE_SET) {
			printf("%s: %"PRIu32"\n",fname,trashtime);
		} else {
			printf("%s: trashtime not changed\n",fname);
		}
	} else {
		printf("%s:\n",fname);
		bsd_humanize_number(hbuf, sizeof(hbuf), changed, "", HN_AUTOSCALE, HN_NOSPACE | HN_DECIMAL);
		printf(" inodes with trashtime changed:      %4s (%"PRIu32")\n",hbuf,changed);
		bsd_humanize_number(hbuf, sizeof(hbuf), notchanged, "", HN_AUTOSCALE, HN_NOSPACE | HN_DECIMAL);
		printf(" inodes with trashtime not changed:  %4s (%"PRIu32")\n",hbuf,notchanged);
		bsd_humanize_number(hbuf, sizeof(hbuf), notpermitted, "", HN_AUTOSCALE, HN_NOSPACE | HN_DECIMAL);
		printf(" inodes with permission denied:      %4s (%"PRIu32")\n",hbuf,notpermitted);
	}
	free(buff);
	return 0;
}

int ip_port_cmp(const void*a,const void*b) {
	return memcmp(a,b,6);
}

int file_info(const char *fname) {
	uint8_t reqbuff[20],*ptr,*buff;
	uint32_t indx,cmd,leng,inode,version;
	uint8_t ip1,ip2,ip3,ip4;
	uint16_t port;
	uint64_t fleng,chunkid;
	int fd;
	fd = open_master_conn(fname,&inode);
	if (fd<0) {
		return -1;
	}
	indx=0;
	do {
		ptr = reqbuff;
		PUT32BIT(CUTOMA_FUSE_READ_CHUNK,ptr);
		PUT32BIT(12,ptr);
		PUT32BIT(0,ptr);
		PUT32BIT(inode,ptr);
		PUT32BIT(indx,ptr);
		if (socket_write(fd,reqbuff,20)!=20) {
			printf("%s [%"PRIu32"]: master query: send error\n",fname,indx);
			close(fd);
			return -1;
		}
		if (socket_read(fd,reqbuff,8)!=8) {
			printf("%s [%"PRIu32"]: master query: receive error\n",fname,indx);
			close(fd);
			return -1;
		}
		ptr = reqbuff;
		GET32BIT(cmd,ptr);
		GET32BIT(leng,ptr);
		if (cmd!=MATOCU_FUSE_READ_CHUNK) {
			printf("%s [%"PRIu32"]: master query: wrong answer (type)\n",fname,indx);
			close(fd);
			return -1;
		}
		buff = malloc(leng);
		if (socket_read(fd,buff,leng)!=(int32_t)leng) {
			printf("%s [%"PRIu32"]: master query: receive error\n",fname,indx);
			free(buff);
			close(fd);
			return -1;
		}
		ptr = buff;
		GET32BIT(cmd,ptr);	// queryid
		if (cmd!=0) {
			printf("%s [%"PRIu32"]: master query: wrong answer (queryid)\n",fname,indx);
			free(buff);
			close(fd);
			return -1;
		}
		leng-=4;
		if (leng==1) {
			printf("%s [%"PRIu32"]: %s\n",fname,indx,errtab[*ptr]);
			free(buff);
			close(fd);
			return -1;
		} else if (leng<20 || ((leng-20)%6)!=0) {
			printf("%s [%"PRIu32"]: master query: wrong answer (leng)\n",fname,indx);
			free(buff);
			close(fd);
			return -1;
		}
		if (indx==0) {
			printf("%s:\n",fname);
		}
		GET64BIT(fleng,ptr);
		GET64BIT(chunkid,ptr);
		GET32BIT(version,ptr);
		if (fleng>0) {
			if (chunkid==0 && version==0) {
				printf("\tchunk %"PRIu32": empty\n",indx);
			} else {
				printf("\tchunk %"PRIu32": %016"PRIX64"_%08"PRIX32" / (id:%"PRIu64" ver:%"PRIu32")\n",indx,chunkid,version,chunkid,version);
				leng-=20;
				leng/=6;
				if (leng>0) {
					qsort(ptr,leng,6,ip_port_cmp);
					for (cmd=0 ; cmd<leng ; cmd++) {
						ip1 = ptr[0];
						ip2 = ptr[1];
						ip3 = ptr[2];
						ip4 = ptr[3];
						ptr+=4;
						GET16BIT(port,ptr);
						printf("\t\tcopy %"PRIu32": %"PRIu8".%"PRIu8".%"PRIu8".%"PRIu8":%"PRIu16"\n",cmd+1,ip1,ip2,ip3,ip4,port);
					}
				} else {
					printf("\t\tno valid copies !!!\n");
				}
			}
		}
		free(buff);
		indx++;
	} while (indx<((fleng+0x3FFFFFF)>>26));
	close(fd);
	return 0;
}

int append_file(const char *fname,const char *afname) {
	uint8_t reqbuff[28],*ptr,*buff;
	uint32_t cmd,leng,inode,ainode,uid,gid;
	int fd;
	fd = open_two_files_master_conn(fname,afname,&inode,&ainode);
	if (fd<0) {
		return -1;
	}
	uid = getuid();
	gid = getgid();
	ptr = reqbuff;
	PUT32BIT(CUTOMA_FUSE_APPEND,ptr);
	PUT32BIT(20,ptr);
	PUT32BIT(0,ptr);
	PUT32BIT(inode,ptr);
	PUT32BIT(ainode,ptr);
	PUT32BIT(uid,ptr);
	PUT32BIT(gid,ptr);
	if (socket_write(fd,reqbuff,28)!=28) {
		printf("%s: master query: send error\n",fname);
		close(fd);
		return -1;
	}
	if (socket_read(fd,reqbuff,8)!=8) {
		printf("%s: master query: receive error\n",fname);
		close(fd);
		return -1;
	}
	ptr = reqbuff;
	GET32BIT(cmd,ptr);
	GET32BIT(leng,ptr);
	if (cmd!=MATOCU_FUSE_APPEND) {
		printf("%s: master query: wrong answer (type)\n",fname);
		close(fd);
		return -1;
	}
	buff = malloc(leng);
	if (socket_read(fd,buff,leng)!=(int32_t)leng) {
		printf("%s: master query: receive error\n",fname);
		free(buff);
		close(fd);
		return -1;
	}
	close(fd);
	ptr = buff;
	GET32BIT(cmd,ptr);	// queryid
	if (cmd!=0) {
		printf("%s: master query: wrong answer (queryid)\n",fname);
		free(buff);
		return -1;
	}
	leng-=4;
	if (leng!=1) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		free(buff);
		return -1;
	} else if (*ptr!=STATUS_OK) {
		printf("%s: %s\n",fname,errtab[*ptr]);
		free(buff);
		return -1;
	}
	free(buff);
	return 0;
}

int dir_info(const char *fname) {
	uint8_t reqbuff[16],*ptr,*buff;
	uint32_t cmd,leng,inode;
	uint32_t inodes,dirs,files,ugfiles,mfiles,chunks,ugchunks,mchunks;
	uint64_t length,size,gsize;
	char hbuf[5];
	int fd;
	fd = open_master_conn(fname,&inode);
	if (fd<0) {
		return -1;
	}
	ptr = reqbuff;
	PUT32BIT(CUTOMA_FUSE_GETDIRSTATS,ptr);
	PUT32BIT(8,ptr);
	PUT32BIT(0,ptr);
	PUT32BIT(inode,ptr);
	if (socket_write(fd,reqbuff,16)!=16) {
		printf("%s: master query: send error\n",fname);
		close(fd);
		return -1;
	}
	if (socket_read(fd,reqbuff,8)!=8) {
		printf("%s: master query: receive error\n",fname);
		close(fd);
		return -1;
	}
	ptr = reqbuff;
	GET32BIT(cmd,ptr);
	GET32BIT(leng,ptr);
	if (cmd!=MATOCU_FUSE_GETDIRSTATS) {
		printf("%s: master query: wrong answer (type)\n",fname);
		close(fd);
		return -1;
	}
	buff = malloc(leng);
	if (socket_read(fd,buff,leng)!=(int32_t)leng) {
		printf("%s: master query: receive error\n",fname);
		free(buff);
		close(fd);
		return -1;
	}
	ptr = buff;
	GET32BIT(cmd,ptr);	// queryid
	if (cmd!=0) {
		printf("%s: master query: wrong answer (queryid)\n",fname);
		free(buff);
		close(fd);
		return -1;
	}
	leng-=4;
	if (leng==1) {
		printf("%s: %s\n",fname,errtab[*ptr]);
		free(buff);
		close(fd);
		return -1;
	} else if (leng!=56) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		free(buff);
		close(fd);
		return -1;
	}
	GET32BIT(inodes,ptr);
	GET32BIT(dirs,ptr);
	GET32BIT(files,ptr);
	GET32BIT(ugfiles,ptr);
	GET32BIT(mfiles,ptr);
	GET32BIT(chunks,ptr);
	GET32BIT(ugchunks,ptr);
	GET32BIT(mchunks,ptr);
	GET64BIT(length,ptr);
	GET64BIT(size,ptr);
	GET64BIT(gsize,ptr);
	printf("%s:\n",fname);
	bsd_humanize_number(hbuf, sizeof(hbuf), inodes, "", HN_AUTOSCALE, HN_NOSPACE | HN_DECIMAL);
	printf(" inodes:                %4s (%"PRIu32")\n",hbuf,inodes);
	bsd_humanize_number(hbuf, sizeof(hbuf), dirs, "", HN_AUTOSCALE, HN_NOSPACE | HN_DECIMAL);
	printf("  directories:          %4s (%"PRIu32")\n",hbuf,dirs);
	bsd_humanize_number(hbuf, sizeof(hbuf), files, "", HN_AUTOSCALE, HN_NOSPACE | HN_DECIMAL);
	printf("  files:                %4s (%"PRIu32")\n",hbuf,files);
	bsd_humanize_number(hbuf, sizeof(hbuf), files-ugfiles-mfiles, "", HN_AUTOSCALE, HN_NOSPACE | HN_DECIMAL);
	printf("   good files:          %4s (%"PRIu32")\n",hbuf,files-ugfiles-mfiles);
	bsd_humanize_number(hbuf, sizeof(hbuf), ugfiles, "", HN_AUTOSCALE, HN_NOSPACE | HN_DECIMAL);
	printf("   under goal files:    %4s (%"PRIu32")\n",hbuf,ugfiles);
	bsd_humanize_number(hbuf, sizeof(hbuf), mfiles, "", HN_AUTOSCALE, HN_NOSPACE | HN_DECIMAL);
	printf("   missing files:       %4s (%"PRIu32")\n",hbuf,mfiles);
	bsd_humanize_number(hbuf, sizeof(hbuf), chunks, "", HN_AUTOSCALE, HN_NOSPACE | HN_DECIMAL);
	printf(" chunks:                %4s (%"PRIu32")\n",hbuf,chunks);
	bsd_humanize_number(hbuf, sizeof(hbuf), chunks-ugchunks-mchunks, "", HN_AUTOSCALE, HN_NOSPACE | HN_DECIMAL);
	printf("  good chunks:          %4s (%"PRIu32")\n",hbuf,chunks-ugchunks-mchunks);
	bsd_humanize_number(hbuf, sizeof(hbuf), ugchunks, "", HN_AUTOSCALE, HN_NOSPACE | HN_DECIMAL);
	printf("  under goal chunks:    %4s (%"PRIu32")\n",hbuf,ugchunks);
	bsd_humanize_number(hbuf, sizeof(hbuf), mchunks, "", HN_AUTOSCALE, HN_NOSPACE | HN_DECIMAL);
	printf("  missing chunks:       %4s (%"PRIu32")\n",hbuf,mchunks);
	bsd_humanize_number(hbuf, sizeof(hbuf), length, "", HN_AUTOSCALE, HN_B | HN_NOSPACE | HN_DECIMAL);
	printf(" length:                %4s (%"PRIu64")\n",hbuf,length);
	bsd_humanize_number(hbuf, sizeof(hbuf), size, "", HN_AUTOSCALE, HN_B | HN_NOSPACE | HN_DECIMAL);
	printf(" size:                  %4s (%"PRIu64")\n",hbuf,size);
	bsd_humanize_number(hbuf, sizeof(hbuf), gsize, "", HN_AUTOSCALE, HN_B | HN_NOSPACE | HN_DECIMAL);
	printf(" hdd usage:             %4s (%"PRIu64")\n",hbuf,gsize);
	free(buff);
	close(fd);
	return 0;
}

int main(int argc,char **argv) {
	int l,f,status;
	unsigned long goal=1,smode=SMODE_SET;
	unsigned long trashtime=86400;
	char *snapshotfname=NULL;

	l = strlen(argv[0]);
	f=0;
	if (l>=8 && strcmp((argv[0])+(l-8),"mfstools")==0) {
		if (argc==2 && strcmp(argv[1],"create")==0) {
			fprintf(stderr,"create symlinks\n");
			symlink(argv[0],"mfsgetgoal");
			symlink(argv[0],"mfssetgoal");
			symlink(argv[0],"mfsrgetgoal");
			symlink(argv[0],"mfsrsetgoal");
			symlink(argv[0],"mfsgettrashtime");
			symlink(argv[0],"mfssettrashtime");
			symlink(argv[0],"mfsrgettrashtime");
			symlink(argv[0],"mfsrsettrashtime");
			symlink(argv[0],"mfscheckfile");
			symlink(argv[0],"mfsfileinfo");
			symlink(argv[0],"mfssnapshot");
			symlink(argv[0],"mfsdirinfo");
			return 0;
		} else {
			fprintf(stderr,"mfs multi tool\n\nusage:\n\tmfstools create - create symlinks (mfs<toolname> -> %s)\n",argv[0]);
			fprintf(stderr,"\ntools:\n");
			fprintf(stderr,"\tmfsgetgoal\n\tmfssetgoal\n\tmfsrgetgoal\n\tmfsrsetgoal\n");
			fprintf(stderr,"\tmfsgettrashtime\n\tmfssettrashtime\n\tmfsrgettrashtime\n\tmfsrsettrashtime\n");
			fprintf(stderr,"\tmfscheckfile\n\tmfsfileinfo\n\tmfssnapshot\n\tmfsdirinfo\n");
			return 1;
		}
	} else if (l>=10 && strcmp((argv[0])+(l-10),"mfsgetgoal")==0) {
		f=1;
	} else if (l>=10 && strcmp((argv[0])+(l-10),"mfssetgoal")==0) {
		f=2;
	} else if (l>=15 && strcmp((argv[0])+(l-15),"mfsgettrashtime")==0) {
		f=3;
	} else if (l>=15 && strcmp((argv[0])+(l-15),"mfssettrashtime")==0) {
		f=4;
	} else if (l>=12 && strcmp((argv[0])+(l-12),"mfscheckfile")==0) {
		f=5;
	} else if (l>=11 && strcmp((argv[0])+(l-11),"mfsfileinfo")==0) {
		f=6;
	} else if (l>=11 && strcmp((argv[0])+(l-11),"mfssnapshot")==0) {
		f=7;
	} else if (l>=10 && strcmp((argv[0])+(l-10),"mfsdirinfo")==0) {
		f=8;
	} else if (l>=11 && strcmp((argv[0])+(l-11),"mfsrgetgoal")==0) {
		f=9;
	} else if (l>=11 && strcmp((argv[0])+(l-11),"mfsrsetgoal")==0) {
		f=10;
	} else if (l>=16 && strcmp((argv[0])+(l-16),"mfsrgettrashtime")==0) {
		f=11;
	} else if (l>=16 && strcmp((argv[0])+(l-16),"mfsrsettrashtime")==0) {
		f=12;
	} else {
		fprintf(stderr,"unknown binary name\n");
		return 1;
	}
	argc--;
	argv++;
	if (((f==2 || f==4 || f==7 || f==10 || f==12) && argc<2) || (f!=2 && f!=4 && f!=7 && f!=10 && f!=12 && argc<1)) {
		switch (f) {
		case 1:
			fprintf(stderr,"get objects goal (desired number of copies)\n\nusage:\n\tmfsgetgoal name [name ...]\n");
			break;
		case 2:
			fprintf(stderr,"set objects goal (desired number of copies)\n\nusage:\n\tmfssetgoal [+|-]GOAL name [name ...]\n\t+GOAL - increase goal to value GOAL\n\t-GOAL - decrease goal to value GOAL\n\tGOAL - just set goal to value GOAL\n");
			break;
		case 3:
			fprintf(stderr,"get objects trashtime (how many seconds file should be left in trash)\n\nusage:\n\tmfsgettrashtime name [name ...]\n");
			break;
		case 4:
			fprintf(stderr,"set objects trashtime (how many seconds file should be left in trash)\n\nusage:\n\tmfssettrashtime SECONDS name [name ...]\n");
			break;
		case 5:
			fprintf(stderr,"check files\n\nusage:\n\tmfscheckfile name [name ...]\n");
			break;
		case 6:
			fprintf(stderr,"show files info (shows detailed info of each file chunk)\n\nusage:\n\tmfsfileinfo name [name ...]\n");
			break;
		case 7:
			fprintf(stderr,"make snapshot (makes virtual copy of file)\n\nusage:\n\tmfssnapshot dstfile name [name ...]\n");
			break;
		case 8:
			fprintf(stderr,"show directories stats\n\nusage:\n\tmfsdirinfo name [name ...]\n\tmeaning of some not obvious output data:\n\t'length' is just sum of files lengths\n\t'size' is sum of chunks lengths\n\t'hdd usgae' is sum of chunks lengths multiplied by number of copies\n");
			break;
		case 9:
			fprintf(stderr,"get objects goal recursively (goal = desired number of copies)\n\nusage:\n\tmfsrgetgoal name [name ...]\n");
			break;
		case 10:
			fprintf(stderr,"set objects goal recursively (goal = desired number of copies)\n\nusage:\n\tmfsrsetgoal [+|-]GOAL name [name ...]\n\t+GOAL - increase goal to value GOAL\n\t-GOAL - decrease goal to value GOAL\n\tGOAL - just set goal to value GOAL\n");
			break;
		case 11:
			fprintf(stderr,"get objects trashtime recursively (trashtime = how many seconds file should be left in trash)\n\nusage:\n\tmfsrgettrashtime name [name ...]\n");
		case 12:
			fprintf(stderr,"set objects trashtime recursively (trashtime = how many seconds file should be left in trash)\n\nusage:\n\tmfsrsettrashtime [+|-]SECONDS name [name ...]\n\t+SECONDS - increase trashtime to value SECONDS\n\t-SECONDS - decrease trashtime to value SECONDS\n\tSECONDS - just set trashtime to value SECONDS\n");
			break;
		}
		return 1;
	}
	if (f==2 || f==4 || f==10 || f==12) {
		int pos;
		pos=0;
		if (argv[0][0]=='+') {
			smode=SMODE_INCREASE;
			pos=1;
		} else if (argv[0][0]=='-') {
			smode=SMODE_DECREASE;
			pos=1;
		}
		if (f==2 || f==10) {
			goal = argv[0][pos]-'0';
			if (goal==0 || goal>9 || argv[0][pos+1]) {
				fprintf(stderr,"goal should be given as a digit between 1 and 9\n");
				return 1;
			}
		} else {
			char *en;
			trashtime = strtoul(argv[0]+pos,&en,10);
			if (*en) {
				fprintf(stderr,"trashtime should be given as number of seconds\n");
				return 1;
			}
		}
		argc--;
		argv++;
	}
	if (f==7) {
		int i;
		snapshotfname = argv[0];
		i = open(snapshotfname,O_RDWR | O_CREAT,0666);
		if (i<0) {
			fprintf(stderr,"can't create/open file: %s\n",snapshotfname);
			return 1;
		}
		close(i);
		argc--;
		argv++;
	}
	status=0;
	while (argc>0) {
		switch (f) {
		case 1:
			if (get_goal(*argv,GMODE_NORMAL)<0) {
				status=1;
			}
			break;
		case 2:
			if (set_goal(*argv,goal,smode)<0) {
				status=1;
			}
			break;
		case 3:
			if (get_trashtime(*argv,GMODE_NORMAL)<0) {
				status=1;
			}
			break;
		case 4:
			if (set_trashtime(*argv,trashtime,smode)<0) {
				status=1;
			}
			break;
		case 5:
			if (check_file(*argv)<0) {
				status=1;
			}
			break;
		case 6:
			if (file_info(*argv)<0) {
				status=1;
			}
			break;
		case 7:
			if (append_file(snapshotfname,*argv)<0) {
				status=1;
			}
			break;
		case 8:
			if (dir_info(*argv)<0) {
				status=1;
			}
			break;
		case 9:
			if (get_goal(*argv,GMODE_RECURSIVE)<0) {
				status=1;
			}
			break;
		case 10:
			if (set_goal(*argv,goal,smode | SMODE_RMASK)<0) {
				status=1;
			}
			break;
		case 11:
			if (get_trashtime(*argv,GMODE_RECURSIVE)<0) {
				status=1;
			}
			break;
		case 12:
			if (set_trashtime(*argv,trashtime,smode | SMODE_RMASK)<0) {
				status=1;
			}
			break;
		}
		argc--;
		argv++;
	}
	return status;
}
