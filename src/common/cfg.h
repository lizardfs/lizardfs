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

#pragma once

#include "common/platform.h"

#include <inttypes.h>
#include <string>

#include "common/slogger.h"

#define _CONFIG_MAKE_PROTOTYPE(fname,type) type cfg_get##fname(const char *name,const type def)

int cfg_load (const char *fname,int logundefined);
int cfg_reload (void);
void cfg_term (void);

/// Returns the name of the currently loaded config file.
std::string cfg_filename();

int cfg_isdefined(const char *name);

_CONFIG_MAKE_PROTOTYPE(str,char*);
_CONFIG_MAKE_PROTOTYPE(string,std::string);
_CONFIG_MAKE_PROTOTYPE(num,int);
_CONFIG_MAKE_PROTOTYPE(uint8,uint8_t);
_CONFIG_MAKE_PROTOTYPE(int8,int8_t);
_CONFIG_MAKE_PROTOTYPE(uint16,uint16_t);
_CONFIG_MAKE_PROTOTYPE(int16,int16_t);
_CONFIG_MAKE_PROTOTYPE(uint32,uint32_t);
_CONFIG_MAKE_PROTOTYPE(int32,int32_t);
_CONFIG_MAKE_PROTOTYPE(uint64,uint64_t);
_CONFIG_MAKE_PROTOTYPE(int64,int64_t);
_CONFIG_MAKE_PROTOTYPE(double,double);

inline uint16_t cfg_get(const char* name, uint16_t defaultValue) {
	return cfg_getuint16(name, defaultValue);
}

inline uint32_t cfg_get(const char* name, uint32_t defaultValue) {
	return cfg_getuint32(name, defaultValue);
}

inline uint64_t cfg_get(const char* name, uint64_t defaultValue) {
	return cfg_getuint64(name, defaultValue);
}

inline double cfg_get(const char* name, double defaultValue) {
	return cfg_getdouble(name, defaultValue);
}

inline double cfg_ranged_get(const char *name, double defaultValue, double lower, double upper) {
	double ret = cfg_getdouble(name, defaultValue);
	if (ret < lower) {
		lzfs_pretty_syslog(LOG_WARNING,
				"Wrong value for %s: Setting min acceptable value %f instead of %f",
				name, lower, ret);
		ret = lower;
	}
	if (ret > upper){
		lzfs_pretty_syslog(LOG_WARNING,
				"Wrong value for %s: Setting max acceptable value %f instead of %f",
				name, upper, ret);
		ret = upper;
	}
	return ret;
}

inline std::string cfg_get(const char* name, const std::string defaultValue) {
	return cfg_getstring(name, defaultValue);
}

template <class T>
T cfg_get_minvalue(const char* name, T defaultValue, T minValue) {
	T configValue = cfg_get(name, defaultValue);
	if (configValue < minValue) {
		lzfs_pretty_syslog(LOG_WARNING, "config value %s was set to %s but minimal value is %s - increasing",
				name, std::to_string(configValue).c_str(), std::to_string(minValue).c_str());
		configValue = minValue;
	}
	return configValue;
}

template <class T>
T cfg_get_maxvalue(const char* name, T defaultValue, T maxValue) {
	T configValue = cfg_get(name, defaultValue);
	if (configValue > maxValue) {
		lzfs_pretty_syslog(LOG_WARNING, "config value %s was set to %s, but maximal value is %s - decreasing",
				name, std::to_string(configValue).c_str(), std::to_string(maxValue).c_str());
		configValue = maxValue;
	}
	return configValue;
}

template <class T>
T cfg_get_minmaxvalue(const char* name, T defaultValue, T minValue, T maxValue) {
	T configValue = cfg_get(name, defaultValue);
	if (configValue < minValue) {
		lzfs_pretty_syslog(LOG_WARNING, "config value %s was set to %s, but minimal value is %s - increasing",
				name, std::to_string(configValue).c_str(), std::to_string(minValue).c_str());
		configValue = minValue;
	} else if (configValue > maxValue) {
		lzfs_pretty_syslog(LOG_WARNING, "config value %s was set to %s, but maximal value is %s - decreasing",
				name, std::to_string(configValue).c_str(), std::to_string(maxValue).c_str());
		configValue = maxValue;
	}
	return configValue;
}

template <class T>
void cfg_warning_on_value_change(const char* name, T expectedValue) {
	T newValue = cfg_get(name, expectedValue);
	if (expectedValue != newValue) {
		lzfs_pretty_syslog(LOG_WARNING, "config value %s has changed, but changing it requires restart", name);
	}
}
