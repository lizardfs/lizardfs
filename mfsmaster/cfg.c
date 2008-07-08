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
#include <string.h>
#include <stdlib.h>
#include <syslog.h>

#include "cfg.h"

typedef struct paramsstr {
	char *name;
	char *value;
	struct paramsstr *next;
} paramstr;

static paramstr *paramhead;

void config_loaddata (FILE *fd) {
	char linebuff[1000];
	uint32_t nps,npe,vps,vpe,i;
	paramstr *tmp;

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
		vpe = i;
		while (linebuff[i]==' ' || linebuff[i]=='\t') i++;
		if (linebuff[i]!='\0' && linebuff[i]!='\r' && linebuff[i]!='\n') {
			continue;
		}
		tmp = (paramstr*)malloc(sizeof(paramstr));
		tmp->name = (char*)malloc(npe-nps+1);
		tmp->value = (char*)malloc(vpe-vps+1);
		memcpy(tmp->name,linebuff+nps,npe-nps);
		memcpy(tmp->value,linebuff+vps,vpe-vps);
		tmp->name[npe-nps]=0;
		tmp->value[vpe-vps]=0;
		tmp->next = paramhead;
		paramhead = tmp;
	}
}

int config_load (const char *configfname) {
	char *varconfigfname;
	uint32_t l;
	FILE *fd;

	paramhead = NULL;

	l = strlen(configfname);
	varconfigfname = malloc(l+5);
	if (varconfigfname==NULL) {
		syslog(LOG_ERR,"cannot allocate memory (new config fname leng : %d)",l+5);
		return 0;
	}
	strcpy(varconfigfname,configfname);
	strcpy(varconfigfname+l,".var");

	fd = fopen(configfname,"r");
	if (fd==NULL) {
		syslog(LOG_ERR,"cannot load config file: %s",configfname);
		free(varconfigfname);
		return 0;
	}
	config_loaddata(fd);
	fclose(fd);

	fd = fopen(varconfigfname,"r");
	if (fd==NULL) {
		syslog(LOG_INFO,"cannot load variable config file: %s",varconfigfname);
	} else {
		config_loaddata(fd);
		fclose(fd);
	}
	free(varconfigfname);
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

#define LOGLINE(name,format,defval) syslog(LOG_NOTICE,"config: using default value for option '%s' - '" format "'",name,defval);
#define NOLOGLINE(name,format,defval)

#define _CONFIG_GET_FUNCTION(fname,type,convname,format,x) \
int config_get##fname(const char *name,type def,type *val) { \
	paramstr *tmp; \
	for (tmp = paramhead ; tmp ; tmp=tmp->next) { \
		if (strcmp(name,tmp->name)==0) { \
			*val = STR_TO_##convname(tmp->value); \
			return 1; \
		} \
	} \
	x(name,format,def) \
	*val = COPY_##convname(def); \
	return 0; \
}

_CONFIG_GET_FUNCTION(newstr,char*,charptr,"%s",LOGLINE)
_CONFIG_GET_FUNCTION(num,int,int,"%d",LOGLINE)
_CONFIG_GET_FUNCTION(int8,int8_t,int32,"%d",LOGLINE)
_CONFIG_GET_FUNCTION(uint8,uint8_t,uint32,"%u",LOGLINE)
_CONFIG_GET_FUNCTION(int16,int16_t,int32,"%d",LOGLINE)
_CONFIG_GET_FUNCTION(uint16,uint16_t,uint32,"%u",LOGLINE)
_CONFIG_GET_FUNCTION(int32,int32_t,int32,"%d",LOGLINE)
_CONFIG_GET_FUNCTION(uint32,uint32_t,uint32,"%u",LOGLINE)
_CONFIG_GET_FUNCTION(int64,int64_t,int64,"%lld",LOGLINE)
_CONFIG_GET_FUNCTION(uint64,uint64_t,uint64,"%llu",LOGLINE)
_CONFIG_GET_FUNCTION(double,double,double,"%lf",LOGLINE)

_CONFIG_GET_FUNCTION(newstr_nolog,char*,charptr,"%s",NOLOGLINE)
_CONFIG_GET_FUNCTION(num_nolog,int,int,"%d",NOLOGLINE)
_CONFIG_GET_FUNCTION(int8_nolog,int8_t,int32,"%d",NOLOGLINE)
_CONFIG_GET_FUNCTION(uint8_nolog,uint8_t,uint32,"%u",NOLOGLINE)
_CONFIG_GET_FUNCTION(int16_nolog,int16_t,int32,"%d",NOLOGLINE)
_CONFIG_GET_FUNCTION(uint16_nolog,uint16_t,uint32,"%u",NOLOGLINE)
_CONFIG_GET_FUNCTION(int32_nolog,int32_t,int32,"%d",NOLOGLINE)
_CONFIG_GET_FUNCTION(uint32_nolog,uint32_t,uint32,"%u",NOLOGLINE)
_CONFIG_GET_FUNCTION(int64_nolog,int64_t,int64,"%lld",NOLOGLINE)
_CONFIG_GET_FUNCTION(uint64_nolog,uint64_t,uint64,"%llu",NOLOGLINE)
_CONFIG_GET_FUNCTION(double_nolog,double,double,"%lf",NOLOGLINE)

