/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"
#include "common/cfg.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>

#include "common/massert.h"
#include "common/slogger.h"

typedef struct paramsstr {
	char *name;
	char *value;
	struct paramsstr *next;
} paramstr;

static char *cfgfname;
static paramstr *paramhead=NULL;
static int logundefined=0;

int cfg_reload(void) {
	int lu = logundefined;
	std::string configfname(cfgfname);
	cfg_term();
	return cfg_load(configfname.c_str(), lu);
}

static int cfg_do_load(void) {
	constexpr size_t MAX_LINE_LEN = 1000;
	char linebuff[MAX_LINE_LEN];
	uint32_t nps, npe, vps, vpe, i;
	uint8_t found;
	paramstr *tmp;
	FILE *fd = fopen(cfgfname, "r");

	if (!fd) {
		lzfs_silent_syslog(LOG_ERR, "can't load config file: %s", cfgfname);
		return 1;
	}

	while (fgets(linebuff, MAX_LINE_LEN - 1, fd)) {
		linebuff[MAX_LINE_LEN - 1] = 0;

		if (linebuff[0] == '#')
			continue;

		for (i = 0; isspace(linebuff[i]); i++);

		for (nps = i; isupper(linebuff[i]) || linebuff[i] == '_'; i++);

		for (npe = i; isspace(linebuff[i]); i++);

		if (linebuff[i] != '=' || npe <= nps) {
			if (linebuff[i] > ' ')
				lzfs_pretty_syslog(LOG_WARNING, "bad "
						"definition in config file "
						"'%s': %s", cfgfname, linebuff);
			continue;
		}

		for (i++; isspace(linebuff[i]); i++);

		for (vps = i; linebuff[i] >= ' ' && linebuff[i] < 127; i++);

		for (; i > vps && linebuff[i - 1] == ' '; i--);

		for (vpe = i; isspace(linebuff[i]); i++);

		if ((linebuff[i] != '\0' && linebuff[i] != '\r' &&
		     linebuff[i] != '\n' && linebuff[i] != '#') || vps == vpe) {

			lzfs_pretty_syslog(LOG_WARNING, "bad definition in "
					"config file '%s': %s", cfgfname,
					linebuff);
			continue;
		}

		linebuff[npe] = 0;
		linebuff[vpe] = 0;
		found = 0;

		for (tmp = paramhead; tmp && !found; tmp = tmp->next) {
			if (strcmp(tmp->name, linebuff + nps))
				continue;

			free(tmp->value);
			tmp->value = (char*)malloc(vpe - vps + 1);
			if (!tmp->value)
				goto err_exit;
			memcpy(tmp->value, linebuff + vps, vpe - vps + 1);
			found = 1;
		}

		if (found)
			continue;

		if (!(tmp = (paramstr*)malloc(sizeof(paramstr))))
			goto err_malloc_tmp;

		if (!(tmp->name = (char*)malloc(npe - nps + 1)))
			goto err_malloc_tmp_name;

		if (!(tmp->value = (char*)malloc(vpe - vps + 1)))
			goto err_malloc_tmp_value;

		memcpy(tmp->name, linebuff + nps, npe - nps + 1);
		memcpy(tmp->value, linebuff + vps, vpe - vps + 1);
		tmp->next = paramhead;
		paramhead = tmp;
	}

	fclose(fd);
	return 0;

err_malloc_tmp_value:
	free(tmp->name);
err_malloc_tmp_name:
	free(tmp);
err_malloc_tmp:
	if (found)
		free(tmp->value);
err_exit:
	fclose(fd);
	cfg_term();
	return -1;
}

int cfg_load (const char *configfname,int _lu) {
	paramhead = NULL;
	logundefined = _lu;
	cfgfname = strdup(configfname);

	return cfg_do_load();
}

std::string cfg_filename() {
	return cfgfname ? cfgfname : "";
}

int cfg_isdefined(const char *name) {
	paramstr *_cfg_tmp;
	for (_cfg_tmp = paramhead ; _cfg_tmp ; _cfg_tmp=_cfg_tmp->next) {
		if (strcmp(name,_cfg_tmp->name)==0) {
			return 1;
		}
	}
	return 0;
}

void cfg_term(void) {
	paramstr *i, *in;
	for (i = paramhead; i; i = in) {
		in = i->next;
		free(i->value);
		free(i->name);
		free(i);
	}
	free(cfgfname);
}

#define STR_TO_int(x) return strtol(x,NULL,0)
#define STR_TO_int32(x) return strtol(x,NULL,0)
#define STR_TO_uint32(x) return strtoul(x,NULL,0)
#define STR_TO_int64(x) return strtoll(x,NULL,0)
#define STR_TO_uint64(x) return strtoull(x,NULL,0)
#define STR_TO_double(x) return strtod(x,NULL)
#define STR_TO_charptr(x) { \
	char* _cfg_ret_tmp = strdup(x); \
	passert(_cfg_ret_tmp); \
	return _cfg_ret_tmp; \
}
#define STR_TO_string(x) return std::string(x)

#define COPY_int(x) return x;
#define COPY_int32(x) return x;
#define COPY_uint32(x) return x;
#define COPY_int64(x) return x;
#define COPY_uint64(x) return x;
#define COPY_double(x) return x;
#define COPY_charptr(x) { \
	char* _cfg_ret_tmp = strdup(x); \
	passert(_cfg_ret_tmp); \
	return _cfg_ret_tmp; \
}
#define COPY_string(x) return x;

#define TOPRINTF_int(x) x
#define TOPRINTF_int32(x) x
#define TOPRINTF_uint32(x) x
#define TOPRINTF_int64(x) x
#define TOPRINTF_uint64(x) x
#define TOPRINTF_double(x) x
#define TOPRINTF_charptr(x) x
#define TOPRINTF_string(x) x.c_str()

#define _CONFIG_GEN_FUNCTION(fname,type,convname,format) \
type cfg_get##fname(const char *name, const type def) { \
	paramstr *_cfg_tmp; \
	for (_cfg_tmp = paramhead ; _cfg_tmp ; _cfg_tmp=_cfg_tmp->next) { \
		if (strcmp(name,_cfg_tmp->name)==0) { \
			STR_TO_##convname(_cfg_tmp->value); \
		} \
	} \
	if (logundefined) { \
		lzfs_pretty_syslog(LOG_NOTICE,"config: using default value for option '%s' - '" format "'", \
				name,TOPRINTF_##convname(def)); \
	} \
	COPY_##convname(def) \
}

_CONFIG_GEN_FUNCTION(str,char*,charptr,"%s")
_CONFIG_GEN_FUNCTION(string,std::string,string,"%s")
_CONFIG_GEN_FUNCTION(num,int,int,"%d")
_CONFIG_GEN_FUNCTION(int8,int8_t,int32,"%" PRId8)
_CONFIG_GEN_FUNCTION(uint8,uint8_t,uint32,"%" PRIu8)
_CONFIG_GEN_FUNCTION(int16,int16_t,int32,"%" PRId16)
_CONFIG_GEN_FUNCTION(uint16,uint16_t,uint32,"%" PRIu16)
_CONFIG_GEN_FUNCTION(int32,int32_t,int32,"%" PRId32)
_CONFIG_GEN_FUNCTION(uint32,uint32_t,uint32,"%" PRIu32)
_CONFIG_GEN_FUNCTION(int64,int64_t,int64,"%" PRId64)
_CONFIG_GEN_FUNCTION(uint64,uint64_t,uint64,"%" PRIu64)
_CONFIG_GEN_FUNCTION(double,double,double,"%f")
