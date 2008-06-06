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
#include <unistd.h>
#include <dirent.h>
#include <string.h>

#include "filesystem.h"
#include "chunks.h"
#include "restore.h"
#include "conf.h"

int lognamecmp(const void *a,const void *b) {
	const char **aa = (const char**)a;
	const char **bb = (const char**)b;
	return strtoul((*bb)+10,NULL,10)-strtoul((*aa)+10,NULL,10);
}

void usage(const char* appname) {
	fprintf(stderr,"restore metadata:\n\t%s -m <meta data file> -o <restored meta data file> [ <change log file> [ <change log file> [ .... ]]\ndump metadata:\n\t%s -m <meta data file>\nautorestore:\n\t%s -a [-d <data path>]\n",appname,appname,appname);
}

int main(int argc,char **argv) {
	char ch;
	int i;
	int autorestore = 0;
	char *metaout = NULL;
	char *metadata = NULL;
	char *datapath = NULL;
	char *chgdata = NULL;
	char *appname = argv[0];
	uint32_t dplen = 0;

	while ((ch = getopt(argc, argv, "m:o:d:a?")) != -1) {
		switch (ch) {
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
		if (datapath==NULL) {
			datapath=strdup(DATA_PATH);
		}
		dplen = strlen(datapath);
		metadata = malloc(dplen+sizeof("/metadata.mfs.back"));
		memcpy(metadata,datapath,dplen);
		memcpy(metadata+dplen,"/metadata.mfs.back",sizeof("/metadata.mfs.back"));
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
		uint32_t files,pos;
		char **fnames;
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
			}
		}
		fnames = (char**)malloc(sizeof(char*)*files);
		pos = 0;
		rewinddir(dd);
		while ((dp = readdir(dd)) != NULL) {
			ptr = dp->d_name;
			if (strncmp(ptr,"changelog.",10)==0) {
				ptr+=10;
				if (*ptr>='0' && *ptr<='9') {
					while (*ptr>='0' && *ptr<='9') {
						ptr++;
					}
					if (strcmp(ptr,".mfs")==0) {
						if (pos<files) {
							fnames[pos]=strdup(dp->d_name);
						}
						pos++;
					}
				}
			}
		}
		closedir(dd);
		qsort(fnames,files,sizeof(char*),lognamecmp);
		for (pos=0 ; pos<files ; pos++) {
			chgdata = malloc(dplen+1+strlen(fnames[pos])+1);
			memcpy(chgdata,datapath,dplen);
			chgdata[dplen]='/';
			memcpy(chgdata+dplen+1,fnames[pos],strlen(fnames[pos])+1);
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
