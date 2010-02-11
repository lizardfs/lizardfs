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
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>

#include "filesystem.h"
#include "chunks.h"
#include "restore.h"

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)
const char id[]="@(#) version: " STR(VERSMAJ) "." STR(VERSMID) "." STR(VERSMIN) ", written by Jakub Kruszona-Zawadzki";

typedef struct _chlogfile {
	char *fname;
	uint16_t setid;
	uint16_t filenum;
} chlogfile;

int chlogfile_cmp(const void *a,const void *b) {
	const chlogfile *aa = (const chlogfile*)a;
	const chlogfile *bb = (const chlogfile*)b;
	if (aa->setid<bb->setid) {
		return -1;
	} else if (aa->setid>bb->setid) {
		return 1;
	} else if (aa->filenum<bb->filenum) {
		return 1;
	} else if (aa->filenum>bb->filenum) {
		return -1;
	}
	return 0;
}

void usage(const char* appname) {
	fprintf(stderr,"restore metadata:\n\t%s -m <meta data file> -o <restored meta data file> [ <change log file> [ <change log file> [ .... ]]\ndump metadata:\n\t%s -m <meta data file>\nautorestore:\n\t%s -a [-d <data path>]\nprint version:\n\t%s -v\n",appname,appname,appname,appname);
}

int main(int argc,char **argv) {
	int ch;
	int i;
	int autorestore = 0;
	char *metaout = NULL;
	char *metadata = NULL;
	char *datapath = NULL;
	char *chgdata = NULL;
	char *appname = argv[0];
	uint32_t dplen = 0;

	while ((ch = getopt(argc, argv, "vm:o:d:a?")) != -1) {
		switch (ch) {
			case 'v':
				printf("version: %u.%u.%u\n",VERSMAJ,VERSMID,VERSMIN);
				return 0;
			case 'o':
				metaout = strdup(optarg);
				break;
			case 'm':
				metadata = strdup(optarg);
				break;
			case 'd':
				datapath = strdup(optarg);
				break;
			case 'a':
				autorestore=1;
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
		return 1;
	}

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
					return 1;
				}
			}
		}
		metaout = malloc(dplen+sizeof("/metadata.mfs"));
		memcpy(metaout,datapath,dplen);
		memcpy(metaout+dplen,"/metadata.mfs",sizeof("/metadata.mfs"));
	}

	if (fs_init(metadata)!=0) {
		printf("can't read metadata from file: %s\n",metadata);
		return 1;
	}

	if (autorestore) {
		DIR *dd;
		struct dirent *dp;
		uint32_t files,pos,filenum;
		chlogfile *logfiles;
		char *ptr;
		dd = opendir(datapath);
		if (!dd) {
			printf("can't open data directory\n");
			return 1;
		}
		files = 0;
		while ((dp = readdir(dd)) != NULL) {
			ptr = dp->d_name;
			if (strncmp(ptr,"changelog.",10)==0) {
				ptr+=10;
				if (*ptr>='0' && *ptr<='9') {
					while (*ptr>='0' && *ptr<='9') {
						ptr++;
					}
					if (strcmp(ptr,".mfs")==0) {
						files++;
					}
				}
			} else if (strncmp(ptr,"changelog_ml.",13)==0) {
				ptr+=13;
				if (*ptr>='0' && *ptr<='9') {
					while (*ptr>='0' && *ptr<='9') {
						ptr++;
					}
					if (strcmp(ptr,".mfs")==0) {
						files++;
					}
				}
			}
		}
		if (files==0) {
			printf("changelog files not found\n");
			return 1;
		}
		logfiles = (chlogfile*)malloc(sizeof(chlogfile)*files);
		pos = 0;
		rewinddir(dd);
		while ((dp = readdir(dd)) != NULL) {
			ptr = dp->d_name;
			if (strncmp(ptr,"changelog.",10)==0) {
				ptr+=10;
				if (*ptr>='0' && *ptr<='9') {
					filenum = strtoul(ptr,&ptr,10);
					if (strcmp(ptr,".mfs")==0) {
						if (pos<files) {
							logfiles[pos].fname=strdup(dp->d_name);
							logfiles[pos].setid=0;
							logfiles[pos].filenum = filenum;
							pos++;
						}
					}
				}
			} else if (strncmp(ptr,"changelog_ml.",13)==0) {
				ptr+=13;
				if (*ptr>='0' && *ptr<='9') {
					filenum = strtoul(ptr,&ptr,10);
					if (strcmp(ptr,".mfs")==0) {
						if (pos<files) {
							logfiles[pos].fname=strdup(dp->d_name);
							logfiles[pos].setid=1;
							logfiles[pos].filenum = filenum;
							pos++;
						}
					}
				}
			}
		}
		closedir(dd);
		qsort(logfiles,files,sizeof(chlogfile),chlogfile_cmp);
		for (pos=0 ; pos<files ; pos++) {
			chgdata = malloc(dplen+1+strlen(logfiles[pos].fname)+1);
			memcpy(chgdata,datapath,dplen);
			chgdata[dplen]='/';
			memcpy(chgdata+dplen+1,logfiles[pos].fname,strlen(logfiles[pos].fname)+1);
			printf("applying changes from file: %s\n",chgdata);
			if (restore(chgdata)!=0) {
				return 1;
			}
			free(chgdata);
		}
	} else {
		for (i=0 ; i<argc ; i++) {
			printf("applying changes from file: %s\n",argv[i]);
			if (restore(argv[i])!=0) {
				return 1;
			}
		}
	}

	if (metaout==NULL) {
		fs_dump();
		chunk_dump();
	} else {
		printf("store metadata into file: %s\n",metaout);
		fs_term(metaout);
	}
	return 0;
}
