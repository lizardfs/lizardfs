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
#include <string.h>
#include <math.h>
#include <limits.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <fcntl.h>
#include <inttypes.h>
#include <poll.h>
#include <errno.h>

#include <limits>

#include "datapack.h"
#include "strerr.h"
#include "mfsstrerr.h"
#include "sockets.h"
#include "MFSCommunication.h"

#define tcpread(s,b,l) tcptoread(s,b,l,10000)
#define tcpwrite(s,b,l) tcptowrite(s,b,l,10000)

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)
const char id[]="@(#) version: " STR(VERSMAJ) "." STR(VERSMID) "." STR(VERSMIN) ", written by Jakub Kruszona-Zawadzki";

#define INODE_VALUE_MASK 0x1FFFFFFF
#define INODE_TYPE_MASK 0x60000000
#define INODE_TYPE_TRASH 0x20000000
#define INODE_TYPE_RESERVED 0x40000000
#define INODE_TYPE_SPECIAL 0x00000000

static const char* eattrtab[EATTR_BITS]={EATTR_STRINGS};
static const char* eattrdesc[EATTR_BITS]={EATTR_DESCRIPTIONS};

static uint8_t humode=0;

#define PHN_USESI       0x01
#define PHN_USEIEC      0x00
void print_humanized_number(const char *format,uint64_t number,uint8_t flags) {
	char numbuf[6];	// [ "xxx" , "xx" , "x" , "x.x" ] + ["" , "X" , "Xi"]
	uint64_t divisor;
	uint16_t b;
	uint8_t i;
	uint8_t scale;

	if (flags & PHN_USESI) {
		divisor = 1000;
	} else {
		divisor = 1024;
	}
	if (number>(std::numeric_limits<uint64_t>::max()/100)) {
		number /= divisor;
		number *= 100;
		scale = 1;
	} else {
		number *= 100;
		scale = 0;
	}
	while (number>=99950) {
		number /= divisor;
		scale+=1;
	}
	i=0;
	if (number<995 && scale>0) {
		b = ((uint32_t)number + 5) / 10;
		numbuf[i++]=(b/10)+'0';
		numbuf[i++]='.';
		numbuf[i++]=(b%10)+'0';
	} else {
		b = ((uint32_t)number + 50) / 100;
		if (b>=100) {
			numbuf[i++]=(b/100)+'0';
			b%=100;
		}
		if (b>=10 || i>0) {
			numbuf[i++]=(b/10)+'0';
			b%=10;
		}
		numbuf[i++]=b+'0';
	}
	if (scale>0) {
		if (flags&PHN_USESI) {
			numbuf[i++]="-kMGTPE"[scale];
		} else {
			numbuf[i++]="-KMGTPE"[scale];
			numbuf[i++]='i';
		}
	}
	numbuf[i++]='\0';
	printf(format,numbuf);
}

void print_number(const char *prefix,const char *suffix,uint64_t number,uint8_t mode32,uint8_t bytesflag,uint8_t dflag) {
	if (prefix) {
		printf("%s",prefix);
	}
	if (dflag) {
		if (humode>0) {
			if (bytesflag) {
				if (humode==1 || humode==3) {
					print_humanized_number("%5sB",number,PHN_USEIEC);
				} else {
					print_humanized_number("%4sB",number,PHN_USESI);
				}
			} else {
				if (humode==1 || humode==3) {
					print_humanized_number(" %5s",number,PHN_USEIEC);
				} else {
					print_humanized_number(" %4s",number,PHN_USESI);
				}
			}
			if (humode>2) {
				if (mode32) {
					printf(" (%10" PRIu32 ")",(uint32_t)number);
				} else {
					printf(" (%20" PRIu64 ")",number);
				}
			}
		} else {
			if (mode32) {
				printf("%10" PRIu32,(uint32_t)number);
			} else {
				printf("%20" PRIu64,number);
			}
		}
	} else {
		switch(humode) {
		case 0:
			if (mode32) {
				printf("         -");
			} else {
				printf("                   -");
			}
			break;
		case 1:
			printf("     -");
			break;
		case 2:
			printf("    -");
			break;
		case 3:
			if (mode32) {
				printf("                  -");
			} else {
				printf("                            -");
			}
			break;
		case 4:
			if (mode32) {
				printf("                 -");
			} else {
				printf("                           -");
			}
			break;
		}
	}
	if (suffix) {
		printf("%s",suffix);
	}
}

int my_get_number(const char *str,uint64_t *ret,double max,uint8_t bytesflag) {
	uint64_t val,frac,fracdiv;
	double drval,mult;
	int f;
	val=0;
	frac=0;
	fracdiv=1;
	f=0;
	while (*str>='0' && *str<='9') {
		f=1;
		val*=10;
		val+=(*str-'0');
		str++;
	}
	if (*str=='.') {	// accept ".5" (without 0)
		str++;
		while (*str>='0' && *str<='9') {
			fracdiv*=10;
			frac*=10;
			frac+=(*str-'0');
			str++;
		}
		if (fracdiv==1) {	// if there was '.' expect number afterwards
			return -1;
		}
	} else if (f==0) {	// but not empty string
		return -1;
	}
	if (str[0]=='\0' || (bytesflag && str[0]=='B' && str[1]=='\0')) {
		mult=1.0;
	} else if (str[0]!='\0' && (str[1]=='\0' || (bytesflag && str[1]=='B' && str[2]=='\0'))) {
		switch(str[0]) {
		case 'k':
			mult=1e3;
			break;
		case 'M':
			mult=1e6;
			break;
		case 'G':
			mult=1e9;
			break;
		case 'T':
			mult=1e12;
			break;
		case 'P':
			mult=1e15;
			break;
		case 'E':
			mult=1e18;
			break;
		default:
			return -1;
		}
	} else if (str[0]!='\0' && str[1]=='i' && (str[2]=='\0' || (bytesflag && str[2]=='B' && str[3]=='\0'))) {
		switch(str[0]) {
		case 'K':
			mult=1024.0;
			break;
		case 'M':
			mult=1048576.0;
			break;
		case 'G':
			mult=1073741824.0;
			break;
		case 'T':
			mult=1099511627776.0;
			break;
		case 'P':
			mult=1125899906842624.0;
			break;
		case 'E':
			mult=1152921504606846976.0;
			break;
		default:
			return -1;
		}
	} else {
		return -1;
	}
	drval = round(((double)frac/(double)fracdiv+(double)val)*mult);
	if (drval>max) {
		return -2;
	} else {
		*ret = drval;
	}
	return 1;
}

int bsd_basename(const char *path,char *bname) {
	const char *endp, *startp;

	/* Empty or NULL string gets treated as "." */
	if (path == NULL || *path == '\0') {
		(void)strcpy(bname, ".");
		return 0;
	}

	/* Strip trailing slashes */
	endp = path + strlen(path) - 1;
	while (endp > path && *endp == '/') {
		endp--;
	}

	/* All slashes becomes "/" */
	if (endp == path && *endp == '/') {
		(void)strcpy(bname, "/");
		return 0;
	}

	/* Find the start of the base */
	startp = endp;
	while (startp > path && *(startp - 1) != '/') {
		startp--;
	}
	if (endp - startp + 2 > PATH_MAX) {
		return -1;
	}
	(void)strncpy(bname, startp, endp - startp + 1);
	bname[endp - startp + 1] = '\0';
	return 0;
}

int bsd_dirname(const char *path,char *bname) {
	const char *endp;

	/* Empty or NULL string gets treated as "." */
	if (path == NULL || *path == '\0') {
		(void)strcpy(bname, ".");
		return 0;
	}

	/* Strip trailing slashes */
	endp = path + strlen(path) - 1;
	while (endp > path && *endp == '/') {
		endp--;
	}

	/* Find the start of the dir */
	while (endp > path && *endp != '/') {
		endp--;
	}

	/* Either the dir is "/" or there are no slashes */
	if (endp == path) {
		(void)strcpy(bname, *endp == '/' ? "/" : ".");
		return 0;
	} else {
		do {
			endp--;
		} while (endp > path && *endp == '/');
	}

	if (endp - path + 2 > PATH_MAX) {
		return -1;
	}
	(void)strncpy(bname, path, endp - path + 1);
	bname[endp - path + 1] = '\0';
	return 0;
}

void dirname_inplace(char *path) {
	char *endp;

	if (path==NULL) {
		return;
	}
	if (path[0]=='\0') {
		path[0]='.';
		path[1]='\0';
		return;
	}

	/* Strip trailing slashes */
	endp = path + strlen(path) - 1;
	while (endp > path && *endp == '/') {
		endp--;
	}

	/* Find the start of the dir */
	while (endp > path && *endp != '/') {
		endp--;
	}

	if (endp == path) {
		if (path[0]=='/') {
			path[1]='\0';
		} else {
			path[0]='.';
			path[1]='\0';
		}
		return;
	} else {
		*endp = '\0';
	}
}

int master_register_old(int rfd) {
	uint32_t i;
	const uint8_t *rptr;
	uint8_t *wptr,regbuff[8+72];

	wptr = regbuff;
	put32bit(&wptr,CLTOMA_FUSE_REGISTER);
	put32bit(&wptr,68);
	memcpy(wptr,FUSE_REGISTER_BLOB_TOOLS_NOACL,64);
	wptr+=64;
	put16bit(&wptr,VERSMAJ);
	put8bit(&wptr,VERSMID);
	put8bit(&wptr,VERSMIN);
	if (tcpwrite(rfd,regbuff,8+68)!=8+68) {
		printf("register to master: send error\n");
		return -1;
	}
	if (tcpread(rfd,regbuff,9)!=9) {
		printf("register to master: receive error\n");
		return -1;
	}
	rptr = regbuff;
	i = get32bit(&rptr);
	if (i!=MATOCL_FUSE_REGISTER) {
		printf("register to master: wrong answer (type)\n");
		return -1;
	}
	i = get32bit(&rptr);
	if (i!=1) {
		printf("register to master: wrong answer (length)\n");
		return -1;
	}
	if (*rptr) {
		printf("register to master: %s\n",mfsstrerr(*rptr));
		return -1;
	}
	return 0;
}

int master_register(int rfd,uint32_t cuid) {
	uint32_t i;
	const uint8_t *rptr;
	uint8_t *wptr,regbuff[8+73];

	wptr = regbuff;
	put32bit(&wptr,CLTOMA_FUSE_REGISTER);
	put32bit(&wptr,73);
	memcpy(wptr,FUSE_REGISTER_BLOB_ACL,64);
	wptr+=64;
	put8bit(&wptr,REGISTER_TOOLS);
	put32bit(&wptr,cuid);
	put16bit(&wptr,VERSMAJ);
	put8bit(&wptr,VERSMID);
	put8bit(&wptr,VERSMIN);
	if (tcpwrite(rfd,regbuff,8+73)!=8+73) {
		printf("register to master: send error\n");
		return -1;
	}
	if (tcpread(rfd,regbuff,9)!=9) {
		printf("register to master: receive error\n");
		return -1;
	}
	rptr = regbuff;
	i = get32bit(&rptr);
	if (i!=MATOCL_FUSE_REGISTER) {
		printf("register to master: wrong answer (type)\n");
		return -1;
	}
	i = get32bit(&rptr);
	if (i!=1) {
		printf("register to master: wrong answer (length)\n");
		return -1;
	}
	if (*rptr) {
		printf("register to master: %s\n",mfsstrerr(*rptr));
		return -1;
	}
	return 0;
}

static dev_t current_device = 0;
static int current_master = -1;
static uint32_t masterversion = 0;

int open_master_conn(const char *name,uint32_t *inode,mode_t *mode,uint8_t needsamedev,uint8_t needrwfs) {
	char rpath[PATH_MAX+1];
	struct stat stb;
	struct statvfs stvfsb;
	int sd;
	uint8_t masterinfo[14];
	const uint8_t *miptr;
	uint8_t cnt;
	uint32_t masterip;
	uint16_t masterport;
	uint32_t mastercuid;

	rpath[0]=0;
	if (realpath(name,rpath)==NULL) {
		printf("%s: realpath error on (%s): %s\n",name,rpath,strerr(errno));
		return -1;
	}
	if (needrwfs) {
		if (statvfs(rpath,&stvfsb)!=0) {
			printf("%s: (%s) statvfs error: %s\n",name,rpath,strerr(errno));
			return -1;
		}
		if (stvfsb.f_flag&ST_RDONLY) {
			printf("%s: (%s) Read-only file system\n",name,rpath);
			return -1;
		}
	}
	if (lstat(rpath,&stb)!=0) {
		printf("%s: (%s) lstat error: %s\n",name,rpath,strerr(errno));
		return -1;
	}
	*inode = stb.st_ino;
	if (mode) {
		*mode = stb.st_mode;
	}
	if (current_master>=0) {
	       	if (current_device==stb.st_dev) {
			return current_master;
		}
		if (needsamedev) {
			printf("%s: different device\n",name);
			return -1;
		}
	}
	if (current_master>=0) {
		close(current_master);
		current_master=-1;
	}
	current_device = stb.st_dev;
	for(;;) {
		if (stb.st_ino==1) {	// found fuse root
			// first try to locate ".masterinfo"
			if (strlen(rpath)+12<PATH_MAX) {
				strcat(rpath,"/.masterinfo");
				if (lstat(rpath,&stb)==0) {
					if ((stb.st_ino==0x7FFFFFFF || stb.st_ino==0x7FFFFFFE) && stb.st_nlink==1 && stb.st_uid==0 && stb.st_gid==0 && (stb.st_size==10 || stb.st_size==14)) {
						if (stb.st_ino==0x7FFFFFFE) {	// meta master
							if (((*inode)&INODE_TYPE_MASK)!=INODE_TYPE_TRASH && ((*inode)&INODE_TYPE_MASK)!=INODE_TYPE_RESERVED) {
								printf("%s: only files in 'trash' and 'reserved' are usable in mfsmeta\n",name);
								return -1;
							}
							(*inode)&=INODE_VALUE_MASK;
						}
						sd = open(rpath,O_RDONLY);
						if (stb.st_size==10) {
							if (read(sd,masterinfo,10)!=10) {
								printf("%s: can't read '.masterinfo'\n",name);
								close(sd);
								return -1;
							}
						} else if (stb.st_size==14) {
							if (read(sd,masterinfo,14)!=14) {
								printf("%s: can't read '.masterinfo'\n",name);
								close(sd);
								return -1;
							}
						}
						close(sd);
						miptr = masterinfo;
						masterip = get32bit(&miptr);
						masterport = get16bit(&miptr);
						mastercuid = get32bit(&miptr);
						if (stb.st_size==14) {
							masterversion = get32bit(&miptr);
						} else {
							masterversion = 0;
						}
						if (masterip==0 || masterport==0 || mastercuid==0) {
							printf("%s: incorrect '.masterinfo'\n",name);
							return -1;
						}
						cnt=0;
						while (cnt<10) {
							sd = tcpsocket();
							if (sd<0) {
								printf("%s: can't create connection socket: %s\n",name,strerr(errno));
								return -1;
							}
							if (tcpnumtoconnect(sd,masterip,masterport,(cnt%2)?(300*(1<<(cnt>>1))):(200*(1<<(cnt>>1))))<0) {
								cnt++;
								if (cnt==10) {
									printf("%s: can't connect to master (.masterinfo): %s\n",name,strerr(errno));
									return -1;
								}
								tcpclose(sd);
							} else {
								cnt=10;
							}
						}
						if (master_register(sd,mastercuid)<0) {
							printf("%s: can't register to master (.masterinfo)\n",name);
							return -1;
						}
						current_master = sd;
						return sd;
					}
				}
				rpath[strlen(rpath)-4]=0;	// cut '.masterinfo' to '.master' and try to fallback to older communication method
				if (lstat(rpath,&stb)==0) {
					if ((stb.st_ino==0x7FFFFFFF || stb.st_ino==0x7FFFFFFE) && stb.st_nlink==1 && stb.st_uid==0 && stb.st_gid==0) {
						if (stb.st_ino==0x7FFFFFFE) {	// meta master
							if (((*inode)&INODE_TYPE_MASK)!=INODE_TYPE_TRASH && ((*inode)&INODE_TYPE_MASK)!=INODE_TYPE_RESERVED) {
								printf("%s: only files in 'trash' and 'reserved' are usable in mfsmeta\n",name);
								return -1;
							}
							(*inode)&=INODE_VALUE_MASK;
						}
						fprintf(stderr,"old version of mfsmount detected - using old and deprecated version of protocol - please upgrade your mfsmount\n");
						sd = open(rpath,O_RDWR);
						if (master_register_old(sd)<0) {
							printf("%s: can't register to master (.master / old protocol)\n",name);
							return -1;
						}
						current_master = sd;
						return sd;
					}
				}
				printf("%s: not MFS object\n",name);
				return -1;
			} else {
				printf("%s: path too long\n",name);
				return -1;
			}
		}
		if (rpath[0]!='/' || rpath[1]=='\0') {
			printf("%s: not MFS object\n",name);
			return -1;
		}
		dirname_inplace(rpath);
		if (lstat(rpath,&stb)!=0) {
			printf("%s: (%s) lstat error: %s\n",name,rpath,strerr(errno));
			return -1;
		}
	}
	return -1;
}

void close_master_conn(int err) {
	if (current_master<0) {
		return;
	}
	if (err) {
		close(current_master);
		current_master = -1;
		current_device = 0;
	}
}

int check_file(const char* fname) {
	uint8_t reqbuff[16],*wptr,*buff;
	const uint8_t *rptr;
	uint32_t cmd,leng,inode;
	uint8_t copies;
	uint32_t chunks;
	int fd;
	fd = open_master_conn(fname,&inode,NULL,0,0);
	if (fd<0) {
		return -1;
	}
	wptr = reqbuff;
	put32bit(&wptr,CLTOMA_FUSE_CHECK);
	put32bit(&wptr,8);
	put32bit(&wptr,0);
	put32bit(&wptr,inode);
	if (tcpwrite(fd,reqbuff,16)!=16) {
		printf("%s: master query: send error\n",fname);
		close_master_conn(1);
		return -1;
	}
	if (tcpread(fd,reqbuff,8)!=8) {
		printf("%s: master query: receive error\n",fname);
		close_master_conn(1);
		return -1;
	}
	rptr = reqbuff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);
	if (cmd!=MATOCL_FUSE_CHECK) {
		printf("%s: master query: wrong answer (type)\n",fname);
		close_master_conn(1);
		return -1;
	}
	buff = (uint8_t*) malloc(leng);
	if (tcpread(fd,buff,leng)!=(int32_t)leng) {
		printf("%s: master query: receive error\n",fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	close_master_conn(0);
	rptr = buff;
	cmd = get32bit(&rptr);	// queryid
	if (cmd!=0) {
		printf("%s: master query: wrong answer (queryid)\n",fname);
		free(buff);
		return -1;
	}
	leng-=4;
	if (leng==1) {
		printf("%s: %s\n",fname,mfsstrerr(*rptr));
		free(buff);
		return -1;
	} else if (leng%3!=0 && leng!=44) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		free(buff);
		return -1;
	}
	printf("%s:\n",fname);
	if (leng%3==0) {
		for (cmd=0 ; cmd<leng ; cmd+=3) {
			copies = get8bit(&rptr);
			chunks = get16bit(&rptr);
			if (copies==1) {
				printf("1 copy:");
			} else {
				printf("%" PRIu8 " copies:",copies);
			}
			print_number(" ","\n",chunks,1,0,1);
		}
	} else {
		for (cmd=0 ; cmd<11 ; cmd++) {
			chunks = get32bit(&rptr);
			if (chunks>0) {
				if (cmd==1) {
					printf(" chunks with 1 copy:    ");
				} else if (cmd>=10) {
					printf(" chunks with 10+ copies:");
				} else {
					printf(" chunks with %u copies:  ",cmd);
				}
				print_number(" ","\n",chunks,1,0,1);
			}
		}
	}
	free(buff);
	return 0;
}

int get_goal(const char *fname,uint8_t mode) {
	uint8_t reqbuff[17],*wptr,*buff;
	const uint8_t *rptr;
	uint32_t cmd,leng,inode;
	uint8_t fn,dn,i;
	uint8_t goal;
	uint32_t cnt;
	int fd;
	fd = open_master_conn(fname,&inode,NULL,0,0);
	if (fd<0) {
		return -1;
	}
	wptr = reqbuff;
	put32bit(&wptr,CLTOMA_FUSE_GETGOAL);
	put32bit(&wptr,9);
	put32bit(&wptr,0);
	put32bit(&wptr,inode);
	put8bit(&wptr,mode);
	if (tcpwrite(fd,reqbuff,17)!=17) {
		printf("%s: master query: send error\n",fname);
		close_master_conn(1);
		return -1;
	}
	if (tcpread(fd,reqbuff,8)!=8) {
		printf("%s: master query: receive error\n",fname);
		close_master_conn(1);
		return -1;
	}
	rptr = reqbuff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);
	if (cmd!=MATOCL_FUSE_GETGOAL) {
		printf("%s: master query: wrong answer (type)\n",fname);
		close_master_conn(1);
		return -1;
	}
	buff = (uint8_t*) malloc(leng);
	if (tcpread(fd,buff,leng)!=(int32_t)leng) {
		printf("%s: master query: receive error\n",fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	close_master_conn(0);
	rptr = buff;
	cmd = get32bit(&rptr);	// queryid
	if (cmd!=0) {
		printf("%s: master query: wrong answer (queryid)\n",fname);
		free(buff);
		return -1;
	}
	leng-=4;
	if (leng==1) {
		printf("%s: %s\n",fname,mfsstrerr(*rptr));
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
		fn = get8bit(&rptr);
		dn = get8bit(&rptr);
		goal = get8bit(&rptr);
		cnt = get32bit(&rptr);
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
		printf("%s: %" PRIu8 "\n",fname,goal);
	} else {
		fn = get8bit(&rptr);
		dn = get8bit(&rptr);
		printf("%s:\n",fname);
		for (i=0 ; i<fn ; i++) {
			goal = get8bit(&rptr);
			cnt = get32bit(&rptr);
			printf(" files with goal        %" PRIu8 " :",goal);
			print_number(" ","\n",cnt,1,0,1);
		}
		for (i=0 ; i<dn ; i++) {
			goal = get8bit(&rptr);
			cnt = get32bit(&rptr);
			printf(" directories with goal  %" PRIu8 " :",goal);
			print_number(" ","\n",cnt,1,0,1);
		}
	}
	free(buff);
	return 0;
}

int get_trashtime(const char *fname,uint8_t mode) {
	uint8_t reqbuff[17],*wptr,*buff;
	const uint8_t *rptr;
	uint32_t cmd,leng,inode;
	uint32_t fn,dn,i;
	uint32_t trashtime;
	uint32_t cnt;
	int fd;
	fd = open_master_conn(fname,&inode,NULL,0,0);
	if (fd<0) {
		return -1;
	}
	wptr = reqbuff;
	put32bit(&wptr,CLTOMA_FUSE_GETTRASHTIME);
	put32bit(&wptr,9);
	put32bit(&wptr,0);
	put32bit(&wptr,inode);
	put8bit(&wptr,mode);
	if (tcpwrite(fd,reqbuff,17)!=17) {
		printf("%s: master query: send error\n",fname);
		close_master_conn(1);
		return -1;
	}
	if (tcpread(fd,reqbuff,8)!=8) {
		printf("%s: master query: receive error\n",fname);
		close_master_conn(1);
		return -1;
	}
	rptr = reqbuff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);
	if (cmd!=MATOCL_FUSE_GETTRASHTIME) {
		printf("%s: master query: wrong answer (type)\n",fname);
		close_master_conn(1);
		return -1;
	}
	buff = (uint8_t*) malloc(leng);
	if (tcpread(fd,buff,leng)!=(int32_t)leng) {
		printf("%s: master query: receive error\n",fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	close_master_conn(0);
	rptr = buff;
	cmd = get32bit(&rptr);	// queryid
	if (cmd!=0) {
		printf("%s: master query: wrong answer (queryid)\n",fname);
		free(buff);
		return -1;
	}
	leng-=4;
	if (leng==1) {
		printf("%s: %s\n",fname,mfsstrerr(*rptr));
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
		fn = get32bit(&rptr);
		dn = get32bit(&rptr);
		trashtime = get32bit(&rptr);
		cnt = get32bit(&rptr);
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
		printf("%s: %" PRIu32 "\n",fname,trashtime);
	} else {
		fn = get32bit(&rptr);
		dn = get32bit(&rptr);
		printf("%s:\n",fname);
		for (i=0 ; i<fn ; i++) {
			trashtime = get32bit(&rptr);
			cnt = get32bit(&rptr);
			printf(" files with trashtime        %10" PRIu32 " :",trashtime);
			print_number(" ","\n",cnt,1,0,1);
		}
		for (i=0 ; i<dn ; i++) {
			trashtime = get32bit(&rptr);
			cnt = get32bit(&rptr);
			printf(" directories with trashtime  %10" PRIu32 " :",trashtime);
			print_number(" ","\n",cnt,1,0,1);
		}
	}
	free(buff);
	return 0;
}

int get_eattr(const char *fname,uint8_t mode) {
	uint8_t reqbuff[17],*wptr,*buff;
	const uint8_t *rptr;
	uint32_t cmd,leng,inode;
	uint8_t fn,dn,i,j;
	uint32_t fcnt[EATTR_BITS];
	uint32_t dcnt[EATTR_BITS];
	uint8_t eattr;
	uint32_t cnt;
	int fd;
	fd = open_master_conn(fname,&inode,NULL,0,0);
	if (fd<0) {
		return -1;
	}
	wptr = reqbuff;
	put32bit(&wptr,CLTOMA_FUSE_GETEATTR);
	put32bit(&wptr,9);
	put32bit(&wptr,0);
	put32bit(&wptr,inode);
	put8bit(&wptr,mode);
	if (tcpwrite(fd,reqbuff,17)!=17) {
		printf("%s: master query: send error\n",fname);
		close_master_conn(1);
		return -1;
	}
	if (tcpread(fd,reqbuff,8)!=8) {
		printf("%s: master query: receive error\n",fname);
		close_master_conn(1);
		return -1;
	}
	rptr = reqbuff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);
	if (cmd!=MATOCL_FUSE_GETEATTR) {
		printf("%s: master query: wrong answer (type)\n",fname);
		close_master_conn(1);
		return -1;
	}
	buff = (uint8_t*) malloc(leng);
	if (tcpread(fd,buff,leng)!=(int32_t)leng) {
		printf("%s: master query: receive error\n",fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	close_master_conn(0);
	rptr = buff;
	cmd = get32bit(&rptr);	// queryid
	if (cmd!=0) {
		printf("%s: master query: wrong answer (queryid)\n",fname);
		free(buff);
		return -1;
	}
	leng-=4;
	if (leng==1) {
		printf("%s: %s\n",fname,mfsstrerr(*rptr));
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
		fn = get8bit(&rptr);
		dn = get8bit(&rptr);
		eattr = get8bit(&rptr);
		cnt = get32bit(&rptr);
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
		printf("%s: ",fname);
		if (eattr>0) {
			cnt=0;
			for (j=0 ; j<EATTR_BITS ; j++) {
				if (eattr & (1<<j)) {
					printf("%s%s",(cnt)?",":"",eattrtab[j]);
					cnt=1;
				}
			}
			printf("\n");
		} else {
			printf("-\n");
		}
//		printf("%s: %" PRIX8 "\n",fname,eattr);
	} else {
		for (j=0 ; j<EATTR_BITS ; j++) {
			fcnt[j]=0;
			dcnt[j]=0;
		}
		fn = get8bit(&rptr);
		dn = get8bit(&rptr);
		for (i=0 ; i<fn ; i++) {
			eattr = get8bit(&rptr);
			cnt = get32bit(&rptr);
			for (j=0 ; j<EATTR_BITS ; j++) {
				if (eattr & (1<<j)) {
					fcnt[j]+=cnt;
				}
			}
		}
		for (i=0 ; i<dn ; i++) {
			eattr = get8bit(&rptr);
			cnt = get32bit(&rptr);
			for (j=0 ; j<EATTR_BITS ; j++) {
				if (eattr & (1<<j)) {
					dcnt[j]+=cnt;
				}
			}
		}
		printf("%s:\n",fname);
		for (j=0 ; j<EATTR_BITS ; j++) {
			if (eattrtab[j][0]) {
				printf(" not directory nodes with attribute %16s :",eattrtab[j]);
				print_number(" ","\n",fcnt[j],1,0,1);
				printf(" directories with attribute         %16s :",eattrtab[j]);
				print_number(" ","\n",dcnt[j],1,0,1);
			} else {
				if (fcnt[j]>0) {
					printf(" not directory nodes with attribute      'unknown-%u' :",j);
					print_number(" ","\n",fcnt[j],1,0,1);
				}
				if (dcnt[j]>0) {
					printf(" directories with attribute              'unknown-%u' :",j);
					print_number(" ","\n",dcnt[j],1,0,1);
				}
			}
		}
	}
	free(buff);
	return 0;
}

int set_goal(const char *fname,uint8_t goal,uint8_t mode) {
	uint8_t reqbuff[22],*wptr,*buff;
	const uint8_t *rptr;
	uint32_t cmd,leng,inode,uid;
	uint32_t changed,notchanged,notpermitted,quotaexceeded;
	int fd;
	fd = open_master_conn(fname,&inode,NULL,0,1);
	if (fd<0) {
		return -1;
	}
	uid = getuid();
	wptr = reqbuff;
	put32bit(&wptr,CLTOMA_FUSE_SETGOAL);
	put32bit(&wptr,14);
	put32bit(&wptr,0);
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	put8bit(&wptr,goal);
	put8bit(&wptr,mode);
	if (tcpwrite(fd,reqbuff,22)!=22) {
		printf("%s: master query: send error\n",fname);
		close_master_conn(1);
		return -1;
	}
	if (tcpread(fd,reqbuff,8)!=8) {
		printf("%s: master query: receive error\n",fname);
		close_master_conn(1);
		return -1;
	}
	rptr = reqbuff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);
	if (cmd!=MATOCL_FUSE_SETGOAL) {
		printf("%s: master query: wrong answer (type)\n",fname);
		close_master_conn(1);
		return -1;
	}
	buff = (uint8_t*) malloc(leng);
	if (tcpread(fd,buff,leng)!=(int32_t)leng) {
		printf("%s: master query: receive error\n",fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	close_master_conn(0);
	rptr = buff;
	cmd = get32bit(&rptr);	// queryid
	if (cmd!=0) {
		printf("%s: master query: wrong answer (queryid)\n",fname);
		free(buff);
		return -1;
	}
	leng-=4;
	if (leng==1) {
		printf("%s: %s\n",fname,mfsstrerr(*rptr));
		free(buff);
		return -1;
	} else if (leng!=12 && leng!=16) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		free(buff);
		return -1;
	}
	changed = get32bit(&rptr);
	notchanged = get32bit(&rptr);
	notpermitted = get32bit(&rptr);
	if (leng==16) {
		quotaexceeded = get32bit(&rptr);
	} else {
		quotaexceeded = 0;
	}
	if ((mode&SMODE_RMASK)==0) {
		if (changed || mode==SMODE_SET) {
			printf("%s: %" PRIu8 "\n",fname,goal);
		} else {
			printf("%s: goal not changed\n",fname);
		}
	} else {
		printf("%s:\n",fname);
		print_number(" inodes with goal changed:      ","\n",changed,1,0,1);
		print_number(" inodes with goal not changed:  ","\n",notchanged,1,0,1);
		print_number(" inodes with permission denied: ","\n",notpermitted,1,0,1);
		if (leng==16) {
			print_number(" inodes with quota exceeded:    ","\n",quotaexceeded,1,0,1);
		}
	}
	free(buff);
	return 0;
}

int set_trashtime(const char *fname,uint32_t trashtime,uint8_t mode) {
	uint8_t reqbuff[25],*wptr,*buff;
	const uint8_t *rptr;
	uint32_t cmd,leng,inode,uid;
	uint32_t changed,notchanged,notpermitted;
	int fd;
	fd = open_master_conn(fname,&inode,NULL,0,1);
	if (fd<0) {
		return -1;
	}
	uid = getuid();
	wptr = reqbuff;
	put32bit(&wptr,CLTOMA_FUSE_SETTRASHTIME);
	put32bit(&wptr,17);
	put32bit(&wptr,0);
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	put32bit(&wptr,trashtime);
	put8bit(&wptr,mode);
	if (tcpwrite(fd,reqbuff,25)!=25) {
		printf("%s: master query: send error\n",fname);
		close_master_conn(1);
		return -1;
	}
	if (tcpread(fd,reqbuff,8)!=8) {
		printf("%s: master query: receive error\n",fname);
		close_master_conn(1);
		return -1;
	}
	rptr = reqbuff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);
	if (cmd!=MATOCL_FUSE_SETTRASHTIME) {
		printf("%s: master query: wrong answer (type)\n",fname);
		close_master_conn(1);
		return -1;
	}
	buff = (uint8_t*) malloc(leng);
	if (tcpread(fd,buff,leng)!=(int32_t)leng) {
		printf("%s: master query: receive error\n",fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	close_master_conn(0);
	rptr = buff;
	cmd = get32bit(&rptr);	// queryid
	if (cmd!=0) {
		printf("%s: master query: wrong answer (queryid)\n",fname);
		free(buff);
		return -1;
	}
	leng-=4;
	if (leng==1) {
		printf("%s: %s\n",fname,mfsstrerr(*rptr));
		free(buff);
		return -1;
	} else if (leng!=12) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		free(buff);
		return -1;
	}
	changed = get32bit(&rptr);
	notchanged = get32bit(&rptr);
	notpermitted = get32bit(&rptr);
	if ((mode&SMODE_RMASK)==0) {
		if (changed || mode==SMODE_SET) {
			printf("%s: %" PRIu32 "\n",fname,trashtime);
		} else {
			printf("%s: trashtime not changed\n",fname);
		}
	} else {
		printf("%s:\n",fname);
		print_number(" inodes with trashtime changed:     ","\n",changed,1,0,1);
		print_number(" inodes with trashtime not changed: ","\n",notchanged,1,0,1);
		print_number(" inodes with permission denied:     ","\n",notpermitted,1,0,1);
	}
	free(buff);
	return 0;
}

int set_eattr(const char *fname,uint8_t eattr,uint8_t mode) {
	uint8_t reqbuff[22],*wptr,*buff;
	const uint8_t *rptr;
	uint32_t cmd,leng,inode,uid;
	uint32_t changed,notchanged,notpermitted;
	int fd;
	fd = open_master_conn(fname,&inode,NULL,0,1);
	if (fd<0) {
		return -1;
	}
	uid = getuid();
	wptr = reqbuff;
	put32bit(&wptr,CLTOMA_FUSE_SETEATTR);
	put32bit(&wptr,14);
	put32bit(&wptr,0);
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	put8bit(&wptr,eattr);
	put8bit(&wptr,mode);
	if (tcpwrite(fd,reqbuff,22)!=22) {
		printf("%s: master query: send error\n",fname);
		close_master_conn(1);
		return -1;
	}
	if (tcpread(fd,reqbuff,8)!=8) {
		printf("%s: master query: receive error\n",fname);
		close_master_conn(1);
		return -1;
	}
	rptr = reqbuff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);
	if (cmd!=MATOCL_FUSE_SETEATTR) {
		printf("%s: master query: wrong answer (type)\n",fname);
		close_master_conn(1);
		return -1;
	}
	buff = (uint8_t*) malloc(leng);
	if (tcpread(fd,buff,leng)!=(int32_t)leng) {
		printf("%s: master query: receive error\n",fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	close_master_conn(0);
	rptr = buff;
	cmd = get32bit(&rptr);	// queryid
	if (cmd!=0) {
		printf("%s: master query: wrong answer (queryid)\n",fname);
		free(buff);
		return -1;
	}
	leng-=4;
	if (leng==1) {
		printf("%s: %s\n",fname,mfsstrerr(*rptr));
		free(buff);
		return -1;
	} else if (leng!=12) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		free(buff);
		return -1;
	}
	changed = get32bit(&rptr);
	notchanged = get32bit(&rptr);
	notpermitted = get32bit(&rptr);
	if ((mode&SMODE_RMASK)==0) {
		if (changed) {
			printf("%s: attribute(s) changed\n",fname);
		} else {
			printf("%s: attribute(s) not changed\n",fname);
		}
	} else {
		printf("%s:\n",fname);
		print_number(" inodes with attributes changed:     ","\n",changed,1,0,1);
		print_number(" inodes with attributes not changed: ","\n",notchanged,1,0,1);
		print_number(" inodes with permission denied:      ","\n",notpermitted,1,0,1);
	}
	free(buff);
	return 0;
}

int ip_port_cmp(const void*a,const void*b) {
	return memcmp(a,b,6);
}

int file_info(const char *fname) {
	uint8_t reqbuff[20],*wptr,*buff;
	const uint8_t *rptr;
	uint32_t indx,cmd,leng,inode,version;
	uint8_t ip1,ip2,ip3,ip4;
	uint16_t port;
	uint64_t fleng,chunkid;
	int fd;
	fd = open_master_conn(fname,&inode,NULL,0,0);
	if (fd<0) {
		return -1;
	}
	indx=0;
	do {
		wptr = reqbuff;
		put32bit(&wptr,CLTOMA_FUSE_READ_CHUNK);
		put32bit(&wptr,12);
		put32bit(&wptr,0);
		put32bit(&wptr,inode);
		put32bit(&wptr,indx);
		if (tcpwrite(fd,reqbuff,20)!=20) {
			printf("%s [%" PRIu32 "]: master query: send error\n",fname,indx);
			close_master_conn(1);
			return -1;
		}
		if (tcpread(fd,reqbuff,8)!=8) {
			printf("%s [%" PRIu32 "]: master query: receive error\n",fname,indx);
			close_master_conn(1);
			return -1;
		}
		rptr = reqbuff;
		cmd = get32bit(&rptr);
		leng = get32bit(&rptr);
		if (cmd!=MATOCL_FUSE_READ_CHUNK) {
			printf("%s [%" PRIu32 "]: master query: wrong answer (type)\n",fname,indx);
			close_master_conn(1);
			return -1;
		}
		buff = (uint8_t*) malloc(leng);
		if (tcpread(fd,buff,leng)!=(int32_t)leng) {
			printf("%s [%" PRIu32 "]: master query: receive error\n",fname,indx);
			free(buff);
			close_master_conn(1);
			return -1;
		}
		rptr = buff;
		cmd = get32bit(&rptr);	// queryid
		if (cmd!=0) {
			printf("%s [%" PRIu32 "]: master query: wrong answer (queryid)\n",fname,indx);
			free(buff);
			close_master_conn(1);
			return -1;
		}
		leng-=4;
		if (leng==1) {
			printf("%s [%" PRIu32 "]: %s\n",fname,indx,mfsstrerr(*rptr));
			free(buff);
			close_master_conn(1);
			return -1;
		} else if (leng<20 || ((leng-20)%6)!=0) {
			printf("%s [%" PRIu32 "]: master query: wrong answer (leng)\n",fname,indx);
			free(buff);
			close_master_conn(1);
			return -1;
		}
		if (indx==0) {
			printf("%s:\n",fname);
		}
		fleng = get64bit(&rptr);
		chunkid = get64bit(&rptr);
		version = get32bit(&rptr);
		if (fleng>0) {
			if (chunkid==0 && version==0) {
				printf("\tchunk %" PRIu32 ": empty\n",indx);
			} else {
				printf("\tchunk %" PRIu32 ": %016" PRIX64 "_%08" PRIX32 " / (id:%" PRIu64 " ver:%" PRIu32 ")\n",indx,chunkid,version,chunkid,version);
				leng-=20;
				leng/=6;
				if (leng>0) {
					wptr = (uint8_t*)rptr;
					qsort(wptr,leng,6,ip_port_cmp);
					for (cmd=0 ; cmd<leng ; cmd++) {
						ip1 = rptr[0];
						ip2 = rptr[1];
						ip3 = rptr[2];
						ip4 = rptr[3];
						rptr+=4;
						port = get16bit(&rptr);
						printf("\t\tcopy %" PRIu32 ": %" PRIu8 ".%" PRIu8 ".%" PRIu8 ".%" PRIu8 ":%" PRIu16 "\n",cmd+1,ip1,ip2,ip3,ip4,port);
					}
				} else {
					printf("\t\tno valid copies !!!\n");
				}
			}
		}
		free(buff);
		indx++;
	} while (indx<((fleng+MFSCHUNKMASK)>>MFSCHUNKBITS));
	close_master_conn(0);
	return 0;
}

int append_file(const char *fname,const char *afname) {
	uint8_t reqbuff[28],*wptr,*buff;
	const uint8_t *rptr;
	uint32_t cmd,leng,inode,ainode,uid,gid;
	mode_t dmode,smode;
	int fd;
	fd = open_master_conn(fname,&inode,&dmode,0,1);
	if (fd<0) {
		return -1;
	}
	if (open_master_conn(afname,&ainode,&smode,1,1)<0) {
		return -1;
	}

	if ((smode&S_IFMT)!=S_IFREG) {
		printf("%s: not a file\n",afname);
		return -1;
	}
	if ((dmode&S_IFMT)!=S_IFREG) {
		printf("%s: not a file\n",fname);
		return -1;
	}
	uid = getuid();
	gid = getgid();
	wptr = reqbuff;
	put32bit(&wptr,CLTOMA_FUSE_APPEND);
	put32bit(&wptr,20);
	put32bit(&wptr,0);
	put32bit(&wptr,inode);
	put32bit(&wptr,ainode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	if (tcpwrite(fd,reqbuff,28)!=28) {
		printf("%s: master query: send error\n",fname);
		close_master_conn(1);
		return -1;
	}
	if (tcpread(fd,reqbuff,8)!=8) {
		printf("%s: master query: receive error\n",fname);
		close_master_conn(1);
		return -1;
	}
	rptr = reqbuff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);
	if (cmd!=MATOCL_FUSE_APPEND) {
		printf("%s: master query: wrong answer (type)\n",fname);
		close_master_conn(1);
		return -1;
	}
	buff = (uint8_t*) malloc(leng);
	if (tcpread(fd,buff,leng)!=(int32_t)leng) {
		printf("%s: master query: receive error\n",fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	close_master_conn(0);
	rptr = buff;
	cmd = get32bit(&rptr);	// queryid
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
	} else if (*rptr!=STATUS_OK) {
		printf("%s: %s\n",fname,mfsstrerr(*rptr));
		free(buff);
		return -1;
	}
	free(buff);
	return 0;
}

int dir_info(const char *fname) {
	uint8_t reqbuff[16],*wptr,*buff;
	const uint8_t *rptr;
	uint32_t cmd,leng,inode;
	uint32_t inodes,dirs,files,chunks;
	uint64_t length,size,realsize;
	int fd;
	fd = open_master_conn(fname,&inode,NULL,0,0);
	if (fd<0) {
		return -1;
	}
	wptr = reqbuff;
	put32bit(&wptr,CLTOMA_FUSE_GETDIRSTATS);
	put32bit(&wptr,8);
	put32bit(&wptr,0);
	put32bit(&wptr,inode);
	if (tcpwrite(fd,reqbuff,16)!=16) {
		printf("%s: master query: send error\n",fname);
		close_master_conn(1);
		return -1;
	}
	if (tcpread(fd,reqbuff,8)!=8) {
		printf("%s: master query: receive error\n",fname);
		close_master_conn(1);
		return -1;
	}
	rptr = reqbuff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);
	if (cmd!=MATOCL_FUSE_GETDIRSTATS) {
		printf("%s: master query: wrong answer (type)\n",fname);
		close_master_conn(1);
		return -1;
	}
	buff = (uint8_t*) malloc(leng);
	if (tcpread(fd,buff,leng)!=(int32_t)leng) {
		printf("%s: master query: receive error\n",fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	rptr = buff;
	cmd = get32bit(&rptr);	// queryid
	if (cmd!=0) {
		printf("%s: master query: wrong answer (queryid)\n",fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	leng-=4;
	if (leng==1) {
		printf("%s: %s\n",fname,mfsstrerr(*rptr));
		free(buff);
		close_master_conn(1);
		return -1;
	} else if (leng!=56 && leng!=40) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	close_master_conn(0);
	inodes = get32bit(&rptr);
	dirs = get32bit(&rptr);
	files = get32bit(&rptr);
	if (leng==56) {
		rptr+=8;
	}
	chunks = get32bit(&rptr);
	if (leng==56) {
		rptr+=8;
	}
	length = get64bit(&rptr);
	size = get64bit(&rptr);
	realsize = get64bit(&rptr);
	free(buff);
	printf("%s:\n",fname);
	print_number(" inodes:       ","\n",inodes,0,0,1);
	print_number("  directories: ","\n",dirs,0,0,1);
	print_number("  files:       ","\n",files,0,0,1);
	print_number(" chunks:       ","\n",chunks,0,0,1);
	print_number(" length:       ","\n",length,0,1,1);
	print_number(" size:         ","\n",size,0,1,1);
	print_number(" realsize:     ","\n",realsize,0,1,1);
	return 0;
}

int file_repair(const char *fname) {
	uint8_t reqbuff[24],*wptr,*buff;
	const uint8_t *rptr;
	uint32_t cmd,leng,inode;
	uint32_t notchanged,erased,repaired;
	int fd;
	fd = open_master_conn(fname,&inode,NULL,0,1);
	if (fd<0) {
		return -1;
	}
	wptr = reqbuff;
	put32bit(&wptr,CLTOMA_FUSE_REPAIR);
	put32bit(&wptr,16);
	put32bit(&wptr,0);
	put32bit(&wptr,inode);
	put32bit(&wptr,getuid());
	put32bit(&wptr,getgid());
	if (tcpwrite(fd,reqbuff,24)!=24) {
		printf("%s: master query: send error\n",fname);
		close_master_conn(1);
		return -1;
	}
	if (tcpread(fd,reqbuff,8)!=8) {
		printf("%s: master query: receive error\n",fname);
		close_master_conn(1);
		return -1;
	}
	rptr = reqbuff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);
	if (cmd!=MATOCL_FUSE_REPAIR) {
		printf("%s: master query: wrong answer (type)\n",fname);
		close_master_conn(1);
		return -1;
	}
	buff = (uint8_t*) malloc(leng);
	if (tcpread(fd,buff,leng)!=(int32_t)leng) {
		printf("%s: master query: receive error\n",fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	rptr = buff;
	cmd = get32bit(&rptr);	// queryid
	if (cmd!=0) {
		printf("%s: master query: wrong answer (queryid)\n",fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	leng-=4;
	if (leng==1) {
		printf("%s: %s\n",fname,mfsstrerr(*rptr));
		free(buff);
		close_master_conn(1);
		return -1;
	} else if (leng!=12) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	close_master_conn(0);
	notchanged = get32bit(&rptr);
	erased = get32bit(&rptr);
	repaired = get32bit(&rptr);
	free(buff);
	printf("%s:\n",fname);
	print_number(" chunks not changed: ","\n",notchanged,1,0,1);
	print_number(" chunks erased:      ","\n",erased,1,0,1);
	print_number(" chunks repaired:    ","\n",repaired,1,0,1);
	return 0;
}

int quota_control(const char *fname,uint8_t del,uint8_t qflags,uint32_t sinodes,uint64_t slength,uint64_t ssize,uint64_t srealsize,uint32_t hinodes,uint64_t hlength,uint64_t hsize,uint64_t hrealsize) {
	uint8_t reqbuff[73],*wptr,*buff;
	const uint8_t *rptr;
	uint32_t cmd,leng,inode;
	uint32_t curinodes;
	uint64_t curlength,cursize,currealsize;
	int fd;
//	printf("set quota: %s (soft:%1X,i:%" PRIu32 ",l:%" PRIu64 ",w:%" PRIu64 ",r:%" PRIu64 "),(hard:%1X,i:%" PRIu32 ",l:%" PRIu64 ",w:%" PRIu64 ",r:%" PRIu64 ")\n",fname,sflags,sinodes,slength,ssize,srealsize,hflags,hinodes,hlength,hsize,hrealsize);
	fd = open_master_conn(fname,&inode,NULL,0,qflags?1:0);
	if (fd<0) {
		return -1;
	}
	wptr = reqbuff;
	put32bit(&wptr,CLTOMA_FUSE_QUOTACONTROL);
	put32bit(&wptr,(del)?9:65);
	put32bit(&wptr,0);
	put32bit(&wptr,inode);
	put8bit(&wptr,qflags);
	if (del==0) {
		put32bit(&wptr,sinodes);
		put64bit(&wptr,slength);
		put64bit(&wptr,ssize);
		put64bit(&wptr,srealsize);
		put32bit(&wptr,hinodes);
		put64bit(&wptr,hlength);
		put64bit(&wptr,hsize);
		put64bit(&wptr,hrealsize);
	}
	if (tcpwrite(fd,reqbuff,(del)?17:73)!=((del)?17:73)) {
		printf("%s: master query: send error\n",fname);
		close_master_conn(1);
		return -1;
	}
	if (tcpread(fd,reqbuff,8)!=8) {
		printf("%s: master query: receive error\n",fname);
		close_master_conn(1);
		return -1;
	}
	rptr = reqbuff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);
	if (cmd!=MATOCL_FUSE_QUOTACONTROL) {
		printf("%s: master query: wrong answer (type)\n",fname);
		close_master_conn(1);
		return -1;
	}
	buff = (uint8_t*) malloc(leng);
	if (tcpread(fd,buff,leng)!=(int32_t)leng) {
		printf("%s: master query: receive error\n",fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	rptr = buff;
	cmd = get32bit(&rptr);	// queryid
	if (cmd!=0) {
		printf("%s: master query: wrong answer (queryid)\n",fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	leng-=4;
	if (leng==1) {
		printf("%s: %s\n",fname,mfsstrerr(*rptr));
		free(buff);
		close_master_conn(1);
		return -1;
	} else if (leng!=85) {
		printf("%s: master query: wrong answer (leng)\n",fname);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	close_master_conn(0);
	qflags = get8bit(&rptr);
	sinodes = get32bit(&rptr);
	slength = get64bit(&rptr);
	ssize = get64bit(&rptr);
	srealsize = get64bit(&rptr);
	hinodes = get32bit(&rptr);
	hlength = get64bit(&rptr);
	hsize = get64bit(&rptr);
	hrealsize = get64bit(&rptr);
	curinodes = get32bit(&rptr);
	curlength = get64bit(&rptr);
	cursize = get64bit(&rptr);
	currealsize = get64bit(&rptr);
	free(buff);
	printf("%s: (current values | soft quota | hard quota)\n",fname);
	print_number(" inodes   | ",NULL,curinodes,0,0,1);
	print_number(" | ",NULL,sinodes,0,0,qflags&QUOTA_FLAG_SINODES);
	print_number(" | "," |\n",hinodes,0,0,qflags&QUOTA_FLAG_HINODES);
	print_number(" length   | ",NULL,curlength,0,1,1);
	print_number(" | ",NULL,slength,0,1,qflags&QUOTA_FLAG_SLENGTH);
	print_number(" | "," |\n",hlength,0,1,qflags&QUOTA_FLAG_HLENGTH);
	print_number(" size     | ",NULL,cursize,0,1,1);
	print_number(" | ",NULL,ssize,0,1,qflags&QUOTA_FLAG_SSIZE);
	print_number(" | "," |\n",hsize,0,1,qflags&QUOTA_FLAG_HSIZE);
	print_number(" realsize | ",NULL,currealsize,0,1,1);
	print_number(" | ",NULL,srealsize,0,1,qflags&QUOTA_FLAG_SREALSIZE);
	print_number(" | "," |\n",hrealsize,0,1,qflags&QUOTA_FLAG_HREALSIZE);
	return 0;
}

int make_snapshot(const char *dstdir,const char *dstbase,const char *srcname,uint32_t srcinode,uint8_t canoverwrite) {
	uint8_t reqbuff[8+22+255],*wptr,*buff;
	const uint8_t *rptr;
	uint32_t cmd,leng,dstinode,uid,gid;
	uint32_t nleng;
	int fd;
	nleng = strlen(dstbase);
	if (nleng>255) {
		printf("%s: name too long\n",dstbase);
		return -1;
	}
	fd = open_master_conn(dstdir,&dstinode,NULL,0,1);
	if (fd<0) {
		return -1;
	}
	uid = getuid();
	gid = getgid();
	wptr = reqbuff;
	put32bit(&wptr,CLTOMA_FUSE_SNAPSHOT);
	put32bit(&wptr,22+nleng);
	put32bit(&wptr,0);
	put32bit(&wptr,srcinode);
	put32bit(&wptr,dstinode);
	put8bit(&wptr,nleng);
	memcpy(wptr,dstbase,nleng);
	wptr+=nleng;
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	put8bit(&wptr,canoverwrite);
	if (tcpwrite(fd,reqbuff,30+nleng)!=(int32_t)(30+nleng)) {
		printf("%s->%s/%s: master query: send error\n",srcname,dstdir,dstbase);
		close_master_conn(1);
		return -1;
	}
	if (tcpread(fd,reqbuff,8)!=8) {
		printf("%s->%s/%s: master query: receive error\n",srcname,dstdir,dstbase);
		close_master_conn(1);
		return -1;
	}
	rptr = reqbuff;
	cmd = get32bit(&rptr);
	leng = get32bit(&rptr);
	if (cmd!=MATOCL_FUSE_SNAPSHOT) {
		printf("%s->%s/%s: master query: wrong answer (type)\n",srcname,dstdir,dstbase);
		close_master_conn(1);
		return -1;
	}
	buff = (uint8_t*) malloc(leng);
	if (tcpread(fd,buff,leng)!=(int32_t)leng) {
		printf("%s->%s/%s: master query: receive error\n",srcname,dstdir,dstbase);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	rptr = buff;
	cmd = get32bit(&rptr);	// queryid
	if (cmd!=0) {
		printf("%s->%s/%s: master query: wrong answer (queryid)\n",srcname,dstdir,dstbase);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	leng-=4;
	if (leng!=1) {
		printf("%s->%s/%s: master query: wrong answer (leng)\n",srcname,dstdir,dstbase);
		free(buff);
		close_master_conn(1);
		return -1;
	}
	close_master_conn(0);
	if (*rptr!=0) {
		printf("%s->%s/%s: %s\n",srcname,dstdir,dstbase,mfsstrerr(*rptr));
		free(buff);
		return -1;
	}
	return 0;
}

int snapshot(const char *dstname,char * const *srcnames,uint32_t srcelements,uint8_t canowerwrite) {
	char to[PATH_MAX+1],base[PATH_MAX+1],dir[PATH_MAX+1];
	char src[PATH_MAX+1];
	struct stat sst,dst;
	int status;
	uint32_t i,l;

	if (stat(dstname,&dst)<0) {	// dst does not exist
		if (errno!=ENOENT) {
			printf("%s: stat error: %s\n",dstname,strerr(errno));
			return -1;
		}
		if (srcelements>1) {
			printf("can snapshot multiple elements only into existing directory\n");
			return -1;
		}
		if (lstat(srcnames[0],&sst)<0) {
			printf("%s: lstat error: %s\n",srcnames[0],strerr(errno));
			return -1;
		}
		if (bsd_dirname(dstname,dir)<0) {
			printf("%s: dirname error\n",dstname);
			return -1;
		}
		if (stat(dir,&dst)<0) {
			printf("%s: stat error: %s\n",dir,strerr(errno));
			return -1;
		}
		if (sst.st_dev != dst.st_dev) {
			printf("(%s,%s): both elements must be on the same device\n",dstname,srcnames[0]);
			return -1;
		}
		if (realpath(dir,to)==NULL) {
			printf("%s: realpath error on %s: %s\n",dir,to,strerr(errno));
			return -1;
		}
		if (bsd_basename(dstname,base)<0) {
			printf("%s: basename error\n",dstname);
			return -1;
		}
		if (strlen(dstname)>0 && dstname[strlen(dstname)-1]=='/' && !S_ISDIR(sst.st_mode)) {
			printf("directory %s does not exist\n",dstname);
			return -1;
		}
		return make_snapshot(to,base,srcnames[0],sst.st_ino,canowerwrite);
	} else {	// dst exists
		if (realpath(dstname,to)==NULL) {
			printf("%s: realpath error on %s: %s\n",dstname,to,strerr(errno));
			return -1;
		}
		if (!S_ISDIR(dst.st_mode)) {	// dst id not a directory
		       	if (srcelements>1) {
				printf("can snapshot multiple elements only into existing directory\n");
				return -1;
			}
			if (lstat(srcnames[0],&sst)<0) {
				printf("%s: lstat error: %s\n",srcnames[0],strerr(errno));
				return -1;
			}
			if (sst.st_dev != dst.st_dev) {
				printf("(%s,%s): both elements must be on the same device\n",dstname,srcnames[0]);
				return -1;
			}
			memcpy(dir,to,PATH_MAX+1);
			dirname_inplace(dir);
			if (bsd_basename(to,base)<0) {
				printf("%s: basename error\n",to);
				return -1;
			}
			return make_snapshot(dir,base,srcnames[0],sst.st_ino,canowerwrite);
		} else {	// dst is a directory
			status = 0;
			for (i=0 ; i<srcelements ; i++) {
				if (lstat(srcnames[i],&sst)<0) {
					printf("%s: lstat error: %s\n",srcnames[i],strerr(errno));
					status=-1;
					continue;
				}
				if (sst.st_dev != dst.st_dev) {
					printf("(%s,%s): both elements must be on the same device\n",dstname,srcnames[i]);
					status=-1;
					continue;
				}
				if (!S_ISDIR(sst.st_mode)) {	// src is not a directory
					if (!S_ISLNK(sst.st_mode)) {	// src is not a symbolic link
						if (realpath(srcnames[i],src)==NULL) {
							printf("%s: realpath error on %s: %s\n",srcnames[i],src,strerr(errno));
							status=-1;
							continue;
						}
						if (bsd_basename(src,base)<0) {
							printf("%s: basename error\n",src);
							status=-1;
							continue;
						}
					} else {	// src is a symbolic link
						if (bsd_basename(srcnames[i],base)<0) {
							printf("%s: basename error\n",srcnames[i]);
							status=-1;
							continue;
						}
					}
					if (make_snapshot(to,base,srcnames[i],sst.st_ino,canowerwrite)<0) {
						status=-1;
					}
				} else {	// src is a directory
					l = strlen(srcnames[i]);
					if (l>0 && srcnames[i][l-1]!='/') {	// src is a directory and name has trailing slash
						if (realpath(srcnames[i],src)==NULL) {
							printf("%s: realpath error on %s: %s\n",srcnames[i],src,strerr(errno));
							status=-1;
							continue;
						}
						if (bsd_basename(src,base)<0) {
							printf("%s: basename error\n",src);
							status=-1;
							continue;
						}
						if (make_snapshot(to,base,srcnames[i],sst.st_ino,canowerwrite)<0) {
							status=-1;
						}
					} else {	// src is a directory and name has not trailing slash
						memcpy(dir,to,PATH_MAX+1);
						dirname_inplace(dir);
						if (bsd_basename(to,base)<0) {
							printf("%s: basename error\n",to);
							status=-1;
							continue;
						}
						if (make_snapshot(dir,base,srcnames[i],sst.st_ino,canowerwrite)<0) {
							status=-1;
						}
					}
				}
			}
			return status;
		}
	}
}

enum {
	MFSGETGOAL=1,
	MFSSETGOAL,
	MFSGETTRASHTIME,
	MFSSETTRASHTIME,
	MFSCHECKFILE,
	MFSFILEINFO,
	MFSAPPENDCHUNKS,
	MFSDIRINFO,
	MFSFILEREPAIR,
	MFSMAKESNAPSHOT,
	MFSGETEATTR,
	MFSSETEATTR,
	MFSDELEATTR,
	MFSGETQUOTA,
	MFSSETQUOTA,
	MFSDELQUOTA
};

static inline void print_numberformat_options() {
	fprintf(stderr," -n - show numbers in plain format\n");
	fprintf(stderr," -h - \"human-readable\" numbers using base 2 prefixes (IEC 60027)\n");
	fprintf(stderr," -H - \"human-readable\" numbers using base 10 prefixes (SI)\n");
}

static inline void print_recursive_option() {
	fprintf(stderr," -r - do it recursively\n");
}

static inline void print_extra_attributes() {
	int j;
	fprintf(stderr,"\nattributes:\n");
	for (j=0 ; j<EATTR_BITS ; j++) {
		if (eattrtab[j][0]) {
			fprintf(stderr," %s - %s\n",eattrtab[j],eattrdesc[j]);
		}
	}
}

void usage(int f) {
	switch (f) {
		case MFSGETGOAL:
			fprintf(stderr,"get objects goal (desired number of copies)\n\nusage: mfsgetgoal [-nhHr] name [name ...]\n");
			print_numberformat_options();
			print_recursive_option();
			break;
		case MFSSETGOAL:
			fprintf(stderr,"set objects goal (desired number of copies)\n\nusage: mfssetgoal [-nhHr] GOAL[-|+] name [name ...]\n");
			print_numberformat_options();
			print_recursive_option();
			fprintf(stderr," GOAL+ - increase goal to given value\n");
			fprintf(stderr," GOAL- - decrease goal to given value\n");
			fprintf(stderr," GOAL - just set goal to given value\n");
			break;
		case MFSGETTRASHTIME:
			fprintf(stderr,"get objects trashtime (how many seconds file should be left in trash)\n\nusage: mfsgettrashtime [-nhHr] name [name ...]\n");
			print_numberformat_options();
			print_recursive_option();
			break;
		case MFSSETTRASHTIME:
			fprintf(stderr,"set objects trashtime (how many seconds file should be left in trash)\n\nusage: mfssettrashtime [-nhHr] SECONDS[-|+] name [name ...]\n");
			print_numberformat_options();
			print_recursive_option();
			fprintf(stderr," SECONDS+ - increase trashtime to given value\n");
			fprintf(stderr," SECONDS- - decrease trashtime to given value\n");
			fprintf(stderr," SECONDS - just set trashtime to given value\n");
			break;
		case MFSCHECKFILE:
			fprintf(stderr,"check files\n\nusage: mfscheckfile [-nhH] name [name ...]\n");
			break;
		case MFSFILEINFO:
			fprintf(stderr,"show files info (shows detailed info of each file chunk)\n\nusage: mfsfileinfo name [name ...]\n");
			break;
		case MFSAPPENDCHUNKS:
			fprintf(stderr,"append file chunks to another file. If destination file doesn't exist then it's created as empty file and then chunks are appended\n\nusage: mfsappendchunks dstfile name [name ...]\n");
			break;
		case MFSDIRINFO:
			fprintf(stderr,"show directories stats\n\nusage: mfsdirinfo [-nhH] name [name ...]\n");
			print_numberformat_options();
			fprintf(stderr,"\nMeaning of some not obvious output data:\n 'length' is just sum of files lengths\n 'size' is sum of chunks lengths\n 'realsize' is estimated hdd usage (usually size multiplied by current goal)\n");
			break;
		case MFSFILEREPAIR:
			fprintf(stderr,"repair given file. Use it with caution. It forces file to be readable, so it could erase (fill with zeros) file when chunkservers are not currently connected.\n\nusage: mfsfilerepair [-nhH] name [name ...]\n");
			print_numberformat_options();
			break;
		case MFSMAKESNAPSHOT:
			fprintf(stderr,"make snapshot (lazy copy)\n\nusage: mfsmakesnapshot [-o] src [src ...] dst\n");
			fprintf(stderr,"-o - allow to overwrite existing objects\n");
			break;
		case MFSGETEATTR:
			fprintf(stderr,"get objects extra attributes\n\nusage: mfsgeteattr [-nhHr] name [name ...]\n");
			print_numberformat_options();
			print_recursive_option();
			break;
		case MFSSETEATTR:
			fprintf(stderr,"set objects extra attributes\n\nusage: mfsseteattr [-nhHr] -f attrname [-f attrname ...] name [name ...]\n");
			print_numberformat_options();
			print_recursive_option();
			fprintf(stderr," -f attrname - specify attribute to set\n");
			print_extra_attributes();
			break;
		case MFSDELEATTR:
			fprintf(stderr,"delete objects extra attributes\n\nusage: mfsdeleattr [-nhHr] -f attrname [-f attrname ...] name [name ...]\n");
			print_numberformat_options();
			print_recursive_option();
			fprintf(stderr," -f attrname - specify attribute to delete\n");
			print_extra_attributes();
			break;
		case MFSGETQUOTA:
			fprintf(stderr,"get quota for given directory (directories)\n\nusage: mfsgetquota [-nhH] dirname [dirname ...]\n");
			print_numberformat_options();
			break;
		case MFSSETQUOTA:
			fprintf(stderr,"set quota for given directory (directories)\n\nusage: mfssetquota [-nhH] [-iI inodes] [-lL length] [-sS size] [-rR realsize] dirname [dirname ...]\n");
			print_numberformat_options();
			fprintf(stderr," -i/-I - set soft/hard limit for number of filesystem objects\n");
			fprintf(stderr," -l/-L - set soft/hard limit for sum of files lengths\n");
			fprintf(stderr," -s/-S - set soft/hard limit for sum of file sizes (chunk sizes)\n");
			fprintf(stderr," -r/-R - set soft/hard limit for estimated hdd usage (usually size multiplied by goal)\n");
			fprintf(stderr,"\nAll numbers can have decimal point and SI/IEC symbol prefix at the end\ndecimal (SI): (k - 10^3 , M - 10^6 , G - 10^9 , T - 10^12 , P - 10^15 , E - 10^18)\nbinary (IEC 60027): (Ki - 2^10 , Mi - 2^20 , Gi - 2^30 , Ti - 2^40 , Pi - 2^50 , Ei - 2^60 )\n");
			break;
		case MFSDELQUOTA:
			fprintf(stderr,"delete quota for given directory (directories)\n\nusage: mfsdelquota [-nhHailsrAILSR] dirname [dirname ...]\n");
			print_numberformat_options();
			fprintf(stderr," -i/-I - delete inodes soft/hard quota\n");
			fprintf(stderr," -l/-L - delete length soft/hard quota\n");
			fprintf(stderr," -s/-S - delete size soft/hard quota\n");
			fprintf(stderr," -r/-R - delete real size soft/hard quota\n");
			fprintf(stderr," -a/-A - delete all soft/hard quotas\n");
			break;
	}
	exit(1);
}

int main(int argc,char **argv) {
	int l,f,status;
	int i,found;
	int ch;
	int oflag=0;
	int rflag=0;
	uint64_t v;
	uint8_t eattr=0,goal=1,smode=SMODE_SET;
	uint32_t trashtime=86400;
	uint32_t sinodes=0,hinodes=0;
	uint64_t slength=0,hlength=0,ssize=0,hsize=0,srealsize=0,hrealsize=0;
	uint8_t qflags=0;
	char *appendfname=NULL;
	char *hrformat;

	strerr_init();

	l = strlen(argv[0]);
#define CHECKNAME(name) ((l==(int)(sizeof(name)-1) && strcmp(argv[0],name)==0) || (l>(int)(sizeof(name)-1) && strcmp((argv[0])+(l-sizeof(name)),"/" name)==0))
	if (CHECKNAME("mfstools")) {
		if (argc==2 && strcmp(argv[1],"create")==0) {
			fprintf(stderr,"create symlinks\n");
#define SYMLINK(name)	if (symlink(argv[0],name)<0) { \
				perror("error creating symlink '" name "'"); \
			}
			SYMLINK("mfsgetgoal")
			SYMLINK("mfssetgoal")
			SYMLINK("mfsgettrashtime")
			SYMLINK("mfssettrashtime")
			SYMLINK("mfscheckfile")
			SYMLINK("mfsfileinfo")
			SYMLINK("mfsappendchunks")
			SYMLINK("mfsdirinfo")
			SYMLINK("mfsfilerepair")
			SYMLINK("mfsmakesnapshot")
			SYMLINK("mfsgeteattr")
			SYMLINK("mfsseteattr")
			SYMLINK("mfsdeleattr")
#if VERSHEX>=0x010700
			SYMLINK("mfsgetquota")
			SYMLINK("mfssetquota")
			SYMLINK("mfsdelquota")
#endif
			// deprecated tools:
			SYMLINK("mfsrgetgoal")
			SYMLINK("mfsrsetgoal")
			SYMLINK("mfsrgettrashtime")
			SYMLINK("mfsrsettrashtime")
			return 0;
		} else {
			fprintf(stderr,"mfs multi tool\n\nusage:\n\tmfstools create - create symlinks (mfs<toolname> -> %s)\n",argv[0]);
			fprintf(stderr,"\ntools:\n");
			fprintf(stderr,"\tmfsgetgoal\n\tmfssetgoal\n\tmfsgettrashtime\n\tmfssettrashtime\n");
			fprintf(stderr,"\tmfscheckfile\n\tmfsfileinfo\n\tmfsappendchunks\n\tmfsdirinfo\n\tmfsfilerepair\n");
			fprintf(stderr,"\tmfsmakesnapshot\n");
			fprintf(stderr,"\tmfsgeteattr\n\tmfsseteattr\n\tmfsdeleattr\n");
#if VERSHEX>=0x010700
			fprintf(stderr,"\tmfsgetquota\n\tmfssetquota\n\tmfsdelquota\n");
#endif
			fprintf(stderr,"\ndeprecated tools:\n");
			fprintf(stderr,"\tmfsrgetgoal = mfsgetgoal -r\n");
			fprintf(stderr,"\tmfsrsetgoal = mfssetgoal -r\n");
			fprintf(stderr,"\tmfsrgettrashtime = mfsgettreshtime -r\n");
			fprintf(stderr,"\tmfsrsettrashtime = mfssettreshtime -r\n");
			return 1;
		}
	} else if (CHECKNAME("mfsgetgoal")) {
		f=MFSGETGOAL;
	} else if (CHECKNAME("mfsrgetgoal")) {
		f=MFSGETGOAL;
		rflag=1;
		fprintf(stderr,"deprecated tool - use \"mfsgetgoal -r\"\n");
	} else if (CHECKNAME("mfssetgoal")) {
		f=MFSSETGOAL;
	} else if (CHECKNAME("mfsrsetgoal")) {
		f=MFSSETGOAL;
		rflag=1;
		fprintf(stderr,"deprecated tool - use \"mfssetgoal -r\"\n");
	} else if (CHECKNAME("mfsgettrashtime")) {
		f=MFSGETTRASHTIME;
	} else if (CHECKNAME("mfsrgettrashtime")) {
		f=MFSGETTRASHTIME;
		rflag=1;
		fprintf(stderr,"deprecated tool - use \"mfsgettrashtime -r\"\n");
	} else if (CHECKNAME("mfssettrashtime")) {
		f=MFSSETTRASHTIME;
	} else if (CHECKNAME("mfsrsettrashtime")) {
		f=MFSSETTRASHTIME;
		rflag=1;
		fprintf(stderr,"deprecated tool - use \"mfssettrashtime -r\"\n");
	} else if (CHECKNAME("mfscheckfile")) {
		f=MFSCHECKFILE;
	} else if (CHECKNAME("mfsfileinfo")) {
		f=MFSFILEINFO;
	} else if (CHECKNAME("mfsappendchunks")) {
		f=MFSAPPENDCHUNKS;
	} else if (CHECKNAME("mfsdirinfo")) {
		f=MFSDIRINFO;
	} else if (CHECKNAME("mfsgeteattr")) {
		f=MFSGETEATTR;
	} else if (CHECKNAME("mfsseteattr")) {
		f=MFSSETEATTR;
	} else if (CHECKNAME("mfsdeleattr")) {
		f=MFSDELEATTR;
	} else if (CHECKNAME("mfsgetquota")) {
		f=MFSGETQUOTA;
	} else if (CHECKNAME("mfssetquota")) {
		f=MFSSETQUOTA;
	} else if (CHECKNAME("mfsdelquota")) {
		f=MFSDELQUOTA;
	} else if (CHECKNAME("mfsfilerepair")) {
		f=MFSFILEREPAIR;
	} else if (CHECKNAME("mfsmakesnapshot")) {
		f=MFSMAKESNAPSHOT;
	} else {
		fprintf(stderr,"unknown binary name\n");
		return 1;
	}

	hrformat = getenv("MFSHRFORMAT");
	if (hrformat) {
		if (hrformat[0]>='0' && hrformat[0]<='4') {
			humode=hrformat[0]-'0';
		}
		if (hrformat[0]=='h') {
			if (hrformat[1]=='+') {
				humode=3;
			} else {
				humode=1;
			}
		}
		if (hrformat[0]=='H') {
			if (hrformat[1]=='+') {
				humode=4;
			} else {
				humode=2;
			}
		}
	}

	// parse options
	switch (f) {
	case MFSMAKESNAPSHOT:
		while ((ch=getopt(argc,argv,"o"))!=-1) {
			switch(ch) {
			case 'o':
				oflag=1;
				break;
			}
		}
		argc -= optind;
		argv += optind;
		if (argc<2) {
			usage(f);
		}
		return snapshot(argv[argc-1],argv,argc-1,oflag);
	case MFSGETGOAL:
	case MFSSETGOAL:
	case MFSGETTRASHTIME:
	case MFSSETTRASHTIME:
		while ((ch=getopt(argc,argv,"rnhH"))!=-1) {
			switch(ch) {
			case 'n':
				humode=0;
				break;
			case 'h':
				humode=1;
				break;
			case 'H':
				humode=2;
				break;
			case 'r':
				rflag=1;
				break;
			}
		}
		argc -= optind;
		argv += optind;
		if ((f==MFSSETGOAL || f==MFSSETTRASHTIME) && argc==0) {
			usage(f);
		}
		if (f==MFSSETGOAL) {
			char *p = argv[0];
			if (p[0]>'0' && p[0]<='9' && (p[1]=='\0' || ((p[1]=='-' || p[1]=='+') && p[2]=='\0'))) {
				goal = p[0]-'0';
				if (p[1]=='-') {
					smode=SMODE_DECREASE;
				} else if (p[1]=='+') {
					smode=SMODE_INCREASE;
				}
			} else {
				fprintf(stderr,"goal should be given as a digit between 1 and 9 optionally folowed by '-' or '+'\n");
				usage(f);
			}
			argc--;
			argv++;
		}
		if (f==MFSSETTRASHTIME) {
			char *p = argv[0];
			trashtime = 0;
			while (p[0]>='0' && p[0]<='9') {
				trashtime*=10;
				trashtime+=(p[0]-'0');
				p++;
			}
			if (p[0]=='\0' || ((p[0]=='-' || p[0]=='+') && p[1]=='\0')) {
				if (p[0]=='-') {
					smode=SMODE_DECREASE;
				} else if (p[0]=='+') {
					smode=SMODE_INCREASE;
				}
			} else {
				fprintf(stderr,"trashtime should be given as number of seconds optionally folowed by '-' or '+'\n");
				usage(f);
			}
			argc--;
			argv++;
		}
		break;
	case MFSGETEATTR:
		while ((ch=getopt(argc,argv,"rnhH"))!=-1) {
			switch(ch) {
			case 'n':
				humode=0;
				break;
			case 'h':
				humode=1;
				break;
			case 'H':
				humode=2;
				break;
			case 'r':
				rflag=1;
				break;
			}
		}
		argc -= optind;
		argv += optind;
		break;
	case MFSSETEATTR:
	case MFSDELEATTR:
		while ((ch=getopt(argc,argv,"rnhHf:"))!=-1) {
			switch(ch) {
			case 'n':
				humode=0;
				break;
			case 'h':
				humode=1;
				break;
			case 'H':
				humode=2;
				break;
			case 'r':
				rflag=1;
				break;
			case 'f':
				found=0;
				for (i=0 ; found==0 && i<EATTR_BITS ; i++) {
					if (strcmp(optarg,eattrtab[i])==0) {
						found=1;
						eattr|=1<<i;
					}
				}
				if (!found) {
					fprintf(stderr,"unknown flag\n");
					usage(f);
				}
				break;
			}
		}
		argc -= optind;
		argv += optind;
		if (eattr==0 && argc>=1) {
			if (f==MFSSETEATTR) {
				fprintf(stderr,"no attribute(s) to set\n");
			} else {
				fprintf(stderr,"no attribute(s) to delete\n");
			}
			usage(f);
		}
		break;
	case MFSFILEREPAIR:
	case MFSGETQUOTA:
	case MFSDIRINFO:
	case MFSCHECKFILE:
		while ((ch=getopt(argc,argv,"nhH"))!=-1) {
			switch(ch) {
			case 'n':
				humode=0;
				break;
			case 'h':
				humode=1;
				break;
			case 'H':
				humode=2;
				break;
			}
		}
		argc -= optind;
		argv += optind;
		break;
	case MFSSETQUOTA:
		if (getuid()) {
			fprintf(stderr,"only root can change quota\n");
			usage(f);
		}
		while ((ch=getopt(argc,argv,"nhHi:I:l:L:s:S:r:R:"))!=-1) {
			switch(ch) {
			case 'n':
				humode=0;
				break;
			case 'h':
				humode=1;
				break;
			case 'H':
				humode=2;
				break;
			case 'i':
				if (my_get_number(optarg,&v,std::numeric_limits<uint32_t>::max(),0)<0) {
					fprintf(stderr,"bad inodes limit\n");
					usage(f);
				}
				if (qflags & QUOTA_FLAG_SINODES) {
					fprintf(stderr,"'soft inodes' quota defined twice\n");
					usage(f);
				}
				sinodes = v;
				qflags |= QUOTA_FLAG_SINODES;
				break;
			case 'I':
				if (my_get_number(optarg,&v,std::numeric_limits<uint32_t>::max(),0)<0) {
					fprintf(stderr,"bad inodes limit\n");
					usage(f);
				}
				if (qflags & QUOTA_FLAG_HINODES) {
					fprintf(stderr,"'hard inodes' quota defined twice\n");
					usage(f);
				}
				hinodes = v;
				qflags |= QUOTA_FLAG_HINODES;
				break;
			case 'l':
				if (my_get_number(optarg,&v,std::numeric_limits<uint64_t>::max(),1)<0) {
					fprintf(stderr,"bad length limit\n");
					usage(f);
				}
				if (qflags & QUOTA_FLAG_SLENGTH) {
					fprintf(stderr,"'soft length' quota defined twice\n");
					usage(f);
				}
				slength = v;
				qflags |= QUOTA_FLAG_SLENGTH;
				break;
			case 'L':
				if (my_get_number(optarg,&v,std::numeric_limits<uint64_t>::max(),1)<0) {
					fprintf(stderr,"bad length limit\n");
					usage(f);
				}
				if (qflags & QUOTA_FLAG_HLENGTH) {
					fprintf(stderr,"'hard length' quota defined twice\n");
					usage(f);
				}
				hlength = v;
				qflags |= QUOTA_FLAG_HLENGTH;
				break;
			case 's':
				if (my_get_number(optarg,&v,std::numeric_limits<uint64_t>::max(),1)<0) {
					fprintf(stderr,"bad size limit\n");
					usage(f);
				}
				if (qflags & QUOTA_FLAG_SSIZE) {
					fprintf(stderr,"'soft size' quota defined twice\n");
					usage(f);
				}
				ssize = v;
				qflags |= QUOTA_FLAG_SSIZE;
				break;
			case 'S':
				if (my_get_number(optarg,&v,std::numeric_limits<uint64_t>::max(),1)<0) {
					fprintf(stderr,"bad size limit\n");
					usage(f);
				}
				if (qflags & QUOTA_FLAG_HSIZE) {
					fprintf(stderr,"'hard size' quota defined twice\n");
					usage(f);
				}
				hsize = v;
				qflags |= QUOTA_FLAG_HSIZE;
				break;
			case 'r':
				if (my_get_number(optarg,&v,std::numeric_limits<uint64_t>::max(),1)<0) {
					fprintf(stderr,"bad real size limit\n");
					usage(f);
				}
				if (qflags & QUOTA_FLAG_SREALSIZE) {
					fprintf(stderr,"'soft realsize' quota defined twice\n");
					usage(f);
				}
				srealsize = v;
				qflags |= QUOTA_FLAG_SREALSIZE;
				break;
			case 'R':
				if (my_get_number(optarg,&v,std::numeric_limits<uint64_t>::max(),1)<0) {
					fprintf(stderr,"bad real size limit\n");
					usage(f);
				}
				if (qflags & QUOTA_FLAG_HREALSIZE) {
					fprintf(stderr,"'hard realsize' quota defined twice\n");
					usage(f);
				}
				hrealsize = v;
				qflags |= QUOTA_FLAG_HREALSIZE;
				break;
			}
		}
		if (qflags==0) {
			fprintf(stderr,"quota options not defined\n");
			usage(f);
		}
		argc -= optind;
		argv += optind;
		break;
	case MFSDELQUOTA:
		if (getuid()) {
			fprintf(stderr,"only root can change quota\n");
			usage(f);
		}
		while ((ch=getopt(argc,argv,"nhHiIlLsSrRaA"))!=-1) {
			switch(ch) {
			case 'n':
				humode=0;
				break;
			case 'h':
				humode=1;
				break;
			case 'H':
				humode=2;
				break;
			case 'i':
				if (qflags & QUOTA_FLAG_SINODES) {
					fprintf(stderr,"'soft inodes' option given twice\n");
					usage(f);
				}
				qflags |= QUOTA_FLAG_SINODES;
				break;
			case 'I':
				if (qflags & QUOTA_FLAG_HINODES) {
					fprintf(stderr,"'hard inodes' option given twice\n");
					usage(f);
				}
				qflags |= QUOTA_FLAG_HINODES;
				break;
			case 'l':
				if (qflags & QUOTA_FLAG_SLENGTH) {
					fprintf(stderr,"'soft length' option given twice\n");
					usage(f);
				}
				qflags |= QUOTA_FLAG_SLENGTH;
				break;
			case 'L':
				if (qflags & QUOTA_FLAG_HLENGTH) {
					fprintf(stderr,"'hard length' option given twice\n");
					usage(f);
				}
				qflags |= QUOTA_FLAG_HLENGTH;
				break;
			case 's':
				if (qflags & QUOTA_FLAG_SSIZE) {
					fprintf(stderr,"'soft size' option given twice\n");
					usage(f);
				}
				qflags |= QUOTA_FLAG_SSIZE;
				break;
			case 'S':
				if (qflags & QUOTA_FLAG_HSIZE) {
					fprintf(stderr,"'hard size' option given twice\n");
					usage(f);
				}
				qflags |= QUOTA_FLAG_HSIZE;
				break;
			case 'r':
				if (qflags & QUOTA_FLAG_SREALSIZE) {
					fprintf(stderr,"'soft realsize' option given twice\n");
					usage(f);
				}
				qflags |= QUOTA_FLAG_SREALSIZE;
				break;
			case 'R':
				if (qflags & QUOTA_FLAG_HREALSIZE) {
					fprintf(stderr,"'hard realsize' option given twice\n");
					usage(f);
				}
				qflags |= QUOTA_FLAG_HREALSIZE;
				break;
			case 'a':
				if (qflags & QUOTA_FLAG_SALL) {
					fprintf(stderr,"'all soft quotas' defined together with other soft quota options\n");
					usage(f);
				}
				qflags |= QUOTA_FLAG_SALL;
				break;
			case 'A':
				if (qflags & QUOTA_FLAG_HALL) {
					fprintf(stderr,"'all hard quotas' defined together with other hard quota options\n");
					usage(f);
				}
				qflags |= QUOTA_FLAG_HALL;
				break;
			}
		}
		if (qflags==0) {
			fprintf(stderr,"quota options not defined\n");
			usage(f);
		}
		argc -= optind;
		argv += optind;
		break;
	default:
		while (getopt(argc,argv,"")!=-1);
		argc -= optind;
		argv += optind;
	}

	if (f==MFSAPPENDCHUNKS) {
		if (argc<=1) {
			usage(f);
		}
		appendfname = argv[0];
		i = open(appendfname,O_RDWR | O_CREAT,0666);
		if (i<0) {
			fprintf(stderr,"can't create/open file: %s\n",appendfname);
			return 1;
		}
		close(i);
		argc--;
		argv++;
	}

	if (argc<1) {
		usage(f);
	}
	status=0;
	while (argc>0) {
		switch (f) {
		case MFSGETGOAL:
			if (get_goal(*argv,(rflag)?GMODE_RECURSIVE:GMODE_NORMAL)<0) {
				status=1;
			}
			break;
		case MFSSETGOAL:
			if (set_goal(*argv,goal,(rflag)?(smode | SMODE_RMASK):smode)<0) {
				status=1;
			}
			break;
		case MFSGETTRASHTIME:
			if (get_trashtime(*argv,(rflag)?GMODE_RECURSIVE:GMODE_NORMAL)<0) {
				status=1;
			}
			break;
		case MFSSETTRASHTIME:
			if (set_trashtime(*argv,trashtime,(rflag)?(smode | SMODE_RMASK):smode)<0) {
				status=1;
			}
			break;
		case MFSCHECKFILE:
			if (check_file(*argv)<0) {
				status=1;
			}
			break;
		case MFSFILEINFO:
			if (file_info(*argv)<0) {
				status=1;
			}
			break;
		case MFSAPPENDCHUNKS:
			if (append_file(appendfname,*argv)<0) {
				status=1;
			}
			break;
		case MFSDIRINFO:
			if (dir_info(*argv)<0) {
				status=1;
			}
			break;
		case MFSFILEREPAIR:
			if (file_repair(*argv)<0) {
				status=1;
			}
			break;
		case MFSGETEATTR:
			if (get_eattr(*argv,(rflag)?GMODE_RECURSIVE:GMODE_NORMAL)<0) {
				status=1;
			}
			break;
		case MFSSETEATTR:
			if (set_eattr(*argv,eattr,(rflag)?(SMODE_RMASK | SMODE_INCREASE):SMODE_INCREASE)<0) {
				status=1;
			}
			break;
		case MFSDELEATTR:
			if (set_eattr(*argv,eattr,(rflag)?(SMODE_RMASK | SMODE_DECREASE):SMODE_DECREASE)<0) {
				status=1;
			}
			break;
		case MFSGETQUOTA:
			if (quota_control(*argv,1,0,0,0,0,0,0,0,0,0)<0) {
				status=1;
			}
			break;
		case MFSSETQUOTA:
			if (quota_control(*argv,0,qflags,sinodes,slength,ssize,srealsize,hinodes,hlength,hsize,hrealsize)<0) {
				status=1;
			}
			break;
		case MFSDELQUOTA:
			if (quota_control(*argv,1,qflags,0,0,0,0,0,0,0,0)<0) {
				status=1;
			}
			break;
		}
		argc--;
		argv++;
	}
	return status;
}
