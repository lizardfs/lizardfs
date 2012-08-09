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
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>
#include <inttypes.h>

#include "filesystem.h"
#include "chunks.h"
#include "merger.h"
#include "restore.h"
#include "strerr.h"

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)
const char id[]="@(#) version: " STR(VERSMAJ) "." STR(VERSMID) "." STR(VERSMIN) ", written by Jakub Kruszona-Zawadzki";

#define MAXIDHOLE 10000

uint64_t findfirstlogversion(const char *fname) {
	uint8_t buff[50];
	int32_t s,p;
	uint64_t fv;
	int fd;

	fd = open(fname,O_RDONLY);
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

uint64_t findlastlogversion(const char *fname) {
	struct stat st;
	uint8_t buff[32800];	// 32800 = 32768 + 32
	uint64_t size;
	uint32_t buffpos;
	uint64_t lastnewline,lv;
	int fd;

	fd = open(fname,O_RDONLY);
	if (fd<0) {
		return 0;
	}
	fstat(fd,&st);
	size = st.st_size;
	memset(buff,0,32);
	lastnewline = 0;
	while (size>0 && size+200000>(uint64_t)(st.st_size)) {
		if (size>32768) {
			memcpy(buff+32768,buff,32);
			size-=32768;
			lseek(fd,size,SEEK_SET);
			if (read(fd,buff,32768)!=32768) {
				close(fd);
				return 0;
			}
			buffpos = 32768;
		} else {
			memmove(buff+size,buff,32);
			lseek(fd,0,SEEK_SET);
			if (read(fd,buff,size)!=(ssize_t)size) {
				close(fd);
				return 0;
			}
			buffpos = size;
			size = 0;
		}
		// size = position in file of first byte in buff
		// buffpos = position of last byte in buff to search
		while (buffpos>0) {
			buffpos--;
			if (buff[buffpos]=='\n') {
				if (lastnewline==0) {
					lastnewline = size + buffpos;
				} else {
					if (lastnewline+1 != (uint64_t)(st.st_size)) {	// garbage at the end of file
						close(fd);
						return 0;
					}
					buffpos++;
					lv = 0;
					while (buffpos<32800 && buff[buffpos]>='0' && buff[buffpos]<='9') {
						lv *= 10;
						lv += buff[buffpos]-'0';
						buffpos++;
					}
					if (buffpos==32800 || buff[buffpos]!=':') {
						lv = 0;
					}
					close(fd);
					return lv;
				}
			}
		}
	}
	close(fd);
	return 0;
}

int changelog_checkname(const char *fname) {
	const char *ptr = fname;
	if (strncmp(ptr,"changelog.",10)==0) {
		ptr+=10;
		if (*ptr>='0' && *ptr<='9') {
			while (*ptr>='0' && *ptr<='9') {
				ptr++;
			}
			if (strcmp(ptr,".mfs")==0) {
				return 1;
			}
		}
	} else if (strncmp(ptr,"changelog_ml.",13)==0) {
		ptr+=13;
		if (*ptr>='0' && *ptr<='9') {
			while (*ptr>='0' && *ptr<='9') {
				ptr++;
			}
			if (strcmp(ptr,".mfs")==0) {
				return 1;
			}
		}
	} else if (strncmp(ptr,"changelog_ml_back.",18)==0) {
		ptr+=18;
		if (*ptr>='0' && *ptr<='9') {
			while (*ptr>='0' && *ptr<='9') {
				ptr++;
			}
			if (strcmp(ptr,".mfs")==0) {
				return 1;
			}
		}
	}
	return 0;
}

void usage(const char* appname) {
	fprintf(stderr,"restore metadata:\n\t%s [-f] [-b] [-i] [-x [-x]] -m <meta data file> -o <restored meta data file> [ <change log file> [ <change log file> [ .... ]]\ndump metadata:\n\t%s [-i] -m <meta data file>\nautorestore:\n\t%s [-f] [-b] [-i] [-x [-x]] -a [-d <data path>]\nprint version:\n\t%s -v\n\n-x - produce more verbose output\n-xx - even more verbose output\n-b - if there is any error in change logs then save the best possible metadata file\n-i - ignore some metadata structure errors (attach orphans to root, ignore names without inode, etc.)\n-f - force loading all changelogs\n",appname,appname,appname,appname);
}

int main(int argc,char **argv) {
	int ch;
	uint8_t vl=0;
//	int i;
	int autorestore = 0;
	int savebest = 0;
	int ignoreflag = 0;
	int forcealllogs = 0;
	int status;
	int skip;
	char *metaout = NULL;
	char *metadata = NULL;
	char *datapath = NULL;
//	char *chgdata = NULL;
	char *appname = argv[0];
	uint32_t dplen = 0;
	uint64_t firstlv,lastlv;

	strerr_init();

	while ((ch = getopt(argc, argv, "fvm:o:d:abxih:?")) != -1) {
		switch (ch) {
			case 'v':
				printf("version: %u.%u.%u\n",VERSMAJ,VERSMID,VERSMIN);
				return 0;
			case 'o':
				if (metaout) {
					free(metaout);
				}
				metaout = strdup(optarg);
				break;
			case 'm':
				if (metadata) {
					free(metadata);
				}
				metadata = strdup(optarg);
				break;
			case 'd':
				if (datapath) {
					free(datapath);
				}
				datapath = strdup(optarg);
				break;
			case 'x':
				vl++;
//				vl = strtoul(optarg,NULL,10);
				break;
			case 'a':
				autorestore=1;
				break;
			case 'b':
				savebest=1;
				break;
			case 'i':
				ignoreflag=1;
				break;
			case 'f':
				forcealllogs=1;
				break;
			case '?':
			default:
				usage(argv[0]);
				return 1;
		}
	}
	argc -= optind;
	argv += optind;

	if ((autorestore==0 && (metadata==NULL || datapath!=NULL)) || (autorestore && (metadata!=NULL || metaout!=NULL))) {
		usage(appname);
		if (datapath) {
			free(datapath);
		}
		if (metadata) {
			free(metadata);
		}
		if (metaout) {
			free(metaout);
		}
		return 1;
	}

	restore_setverblevel(vl);

	if (autorestore) {
		struct stat metast;
		if (datapath==NULL) {
			datapath=strdup(DATA_PATH);
		}
		dplen = strlen(datapath);
		metadata = malloc(dplen+sizeof("/metadata.mfs.back"));
		memcpy(metadata,datapath,dplen);
		memcpy(metadata+dplen,"/metadata.mfs.back",sizeof("/metadata.mfs.back"));
		if (stat(metadata,&metast)<0) {
			if (errno==ENOENT) {
				free(metadata);
				metadata = malloc(dplen+sizeof("/metadata_ml.mfs.back"));
				memcpy(metadata,datapath,dplen);
				memcpy(metadata+dplen,"/metadata_ml.mfs.back",sizeof("/metadata_ml.mfs.back"));
				if (stat(metadata,&metast)==0) {
					printf("file 'metadata.mfs.back' not found - will try 'metadata_ml.mfs.back' instead\n");
				} else {
					printf("can't find backed up metadata file !!!\n");
					if (datapath) {
						free(datapath);
					}
					if (metadata) {
						free(metadata);
					}
					if (metaout) {
						free(metaout);
					}
					return 1;
				}
			}
		}
		metaout = malloc(dplen+sizeof("/metadata.mfs"));
		memcpy(metaout,datapath,dplen);
		memcpy(metaout+dplen,"/metadata.mfs",sizeof("/metadata.mfs"));
	}

	if (fs_init(metadata,ignoreflag)!=0) {
		printf("can't read metadata from file: %s\n",metadata);
		if (datapath) {
			free(datapath);
		}
		if (metadata) {
			free(metadata);
		}
		if (metaout) {
			free(metaout);
		}
		return 1;
	}

	if (autorestore) {
		DIR *dd;
		struct dirent *dp;
		uint32_t files,pos,nlen;
		char **filenames;

		dd = opendir(datapath);
		if (!dd) {
			printf("can't open data directory\n");
			if (datapath) {
				free(datapath);
			}
			if (metadata) {
				free(metadata);
			}
			if (metaout) {
				free(metaout);
			}
			return 1;
		}
		files = 0;
		while ((dp = readdir(dd)) != NULL) {
			files += changelog_checkname(dp->d_name);
		}
		if (files==0) {
			printf("changelog files not found\n");
			closedir(dd);
			if (datapath) {
				free(datapath);
			}
			if (metadata) {
				free(metadata);
			}
			if (metaout) {
				free(metaout);
			}
			return 1;
		}
		filenames = (char**)malloc(sizeof(char*)*files);
		pos = 0;
		rewinddir(dd);
		while ((dp = readdir(dd)) != NULL) {
			if (changelog_checkname(dp->d_name)) {
				nlen = strlen(dp->d_name);
				filenames[pos] = malloc(dplen+1+nlen+1);
				memcpy(filenames[pos],datapath,dplen);
				filenames[pos][dplen]='/';
				memcpy(filenames[pos]+dplen+1,dp->d_name,nlen);
				filenames[pos][dplen+nlen+1]=0;
				firstlv = findfirstlogversion(filenames[pos]);
				lastlv = findlastlogversion(filenames[pos]);
				skip = ((lastlv<fs_getversion() || firstlv==0) && forcealllogs==0)?1:0;
				if (vl>0) {
					if (skip) {
						printf("skipping changelog file: %s (changes: ",filenames[pos]);
					} else {
						printf("using changelog file: %s (changes: ",filenames[pos]);
					}
					if (firstlv>0) {
						printf("%"PRIu64" - ",firstlv);
					} else {
						printf("??? - ");
					}
					if (lastlv>0) {
						printf("%"PRIu64")\n",lastlv);
					} else {
						printf("?\?\?)\n");
					}
				}
				if (skip) {
					free(filenames[pos]);
					files--;
				} else {
					pos++;
				}
			}
		}
		closedir(dd);
		merger_start(files,filenames,MAXIDHOLE);
		for (pos = 0 ; pos<files ; pos++) {
			free(filenames[pos]);
		}
		free(filenames);
	} else {
		uint32_t files,pos;
		char **filenames;

		filenames = (char**)malloc(sizeof(char*)*argc);
		files = 0;
		for (pos=0 ; (int32_t)pos<argc ; pos++) {
			firstlv = findfirstlogversion(argv[pos]);
			lastlv = findlastlogversion(argv[pos]);
			skip = ((lastlv<fs_getversion() || firstlv==0) && forcealllogs==0)?1:0;
			if (vl>0) {
				if (skip) {
					printf("skipping changelog file: %s (changes: ",argv[pos]);
				} else {
					printf("using changelog file: %s (changes: ",argv[pos]);
				}
				if (firstlv>0) {
					printf("%"PRIu64" - ",firstlv);
				} else {
					printf("??? - ");
				}
				if (lastlv>0) {
					printf("%"PRIu64")\n",lastlv);
				} else {
					printf("?\?\?)\n");
				}
			}
			if (skip==0) {
				filenames[files] = strdup(argv[pos]);
				files++;
			}
		}
		merger_start(files,filenames,MAXIDHOLE);
		for (pos = 0 ; pos<files ; pos++) {
			free(filenames[pos]);
		}
		free(filenames);
	}

	status = merger_loop();

	if (status<0 && savebest==0) {
		if (datapath) {
			free(datapath);
		}
		if (metadata) {
			free(metadata);
		}
		if (metaout) {
			free(metaout);
		}
		return 1;
	}

	if (metaout==NULL) {
		fs_dump();
		chunk_dump();
	} else {
		printf("store metadata into file: %s\n",metaout);
		fs_term(metaout);
	}
	if (datapath) {
		free(datapath);
	}
	if (metadata) {
		free(metadata);
	}
	if (metaout) {
		free(metaout);
	}
	return 0;
}
