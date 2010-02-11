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
#include <stdlib.h>
#include <syslog.h>

#include "cfg.h"

typedef struct paramsstr {
	char *name;
	char *value;
	struct paramsstr *next;
} paramstr;

static paramstr *paramhead=NULL;
static int logundefined=0;

int cfg_load (const char *configfname,int _lu) {
	FILE *fd;
	char linebuff[1000];
	uint32_t nps,npe,vps,vpe,i;

	paramstr *tmp;
	paramhead = NULL;
	logundefined = _lu;

	fd = fopen(configfname,"r");
	if (fd==NULL) {
		syslog(LOG_ERR,"cannot load config file: %s",configfname);
		return 0;
	}
	while (fgets(linebuff,999,fd)!=NULL) {
		linebuff[999]=0;
		if (linebuff[0]=='#') {
			continue;
		}
		i = 0;
		while (linebuff[i]==' ' || linebuff[i]=='\t') i++;
		nps = i;
		while (linebuff[i]>32 && linebuff[i]<127) {
			i++;
		}
		npe = i;
		while (linebuff[i]==' ' || linebuff[i]=='\t') i++;
		if (linebuff[i]!='=' || npe==nps) {
			continue;
		}
		i++;
		while (linebuff[i]==' ' || linebuff[i]=='\t') i++;
		vps = i;
		while (linebuff[i]>=32 && linebuff[i]<127) {
			i++;
		}
		while (i>vps && linebuff[i-1]==32) {
			i--;
		}
		vpe = i;
		while (linebuff[i]==' ' || linebuff[i]=='\t') i++;
		if (linebuff[i]!='\0' && linebuff[i]!='\r' && linebuff[i]!='\n') {
			continue;
		}
		tmp = (paramstr*)malloc(sizeof(paramstr));
		tmp->name = (char*)malloc(npe-nps+1);
		tmp->value = (char*)malloc(vpe-vps+1);
		memcpy(tmp->name,linebuff+nps,npe-nps);
		if (vpe>vps) {
			memcpy(tmp->value,linebuff+vps,vpe-vps);
		}
		tmp->name[npe-nps]=0;
		tmp->value[vpe-vps]=0;
		tmp->next = paramhead;
		paramhead = tmp;
	}
	fclose(fd);
	return 1;
}

#define STR_TO_int(x) strtol(x,NULL,0)
#define STR_TO_int32(x) strtol(x,NULL,0)
#define STR_TO_uint32(x) strtoul(x,NULL,0)
#define STR_TO_int64(x) strtoll(x,NULL,0)
#define STR_TO_uint64(x) strtoull(x,NULL,0)
#define STR_TO_double(x) strtod(x,NULL)
#define STR_TO_charptr(x) strdup(x)

#define COPY_int(x) x
#define COPY_int32(x) x
#define COPY_uint32(x) x
#define COPY_int64(x) x
#define COPY_uint64(x) x
#define COPY_double(x) x
#define COPY_charptr(x) strdup(x)

#define _CONFIG_GEN_FUNCTION(fname,type,convname,format) \
type cfg_get##fname(const char *name,type def) { \
	paramstr *tmp; \
	for (tmp = paramhead ; tmp ; tmp=tmp->next) { \
		if (strcmp(name,tmp->name)==0) { \
			return STR_TO_##convname(tmp->value); \
		} \
	} \
	if (logundefined) { \
		syslog(LOG_NOTICE,"config: using default value for option '%s' - '" format "'",name,def); \
	} \
	return COPY_##convname(def); \
}

_CONFIG_GEN_FUNCTION(str,char*,charptr,"%s")
_CONFIG_GEN_FUNCTION(num,int,int,"%d")
_CONFIG_GEN_FUNCTION(int8,int8_t,int32,"%"PRId8)
_CONFIG_GEN_FUNCTION(uint8,uint8_t,uint32,"%"PRIu8)
_CONFIG_GEN_FUNCTION(int16,int16_t,int32,"%"PRId16)
_CONFIG_GEN_FUNCTION(uint16,uint16_t,uint32,"%"PRIu16)
_CONFIG_GEN_FUNCTION(int32,int32_t,int32,"%"PRId32)
_CONFIG_GEN_FUNCTION(uint32,uint32_t,uint32,"%"PRIu32)
_CONFIG_GEN_FUNCTION(int64,int64_t,int64,"%"PRId64)
_CONFIG_GEN_FUNCTION(uint64,uint64_t,uint64,"%"PRIu64)
_CONFIG_GEN_FUNCTION(double,double,double,"%lf")
