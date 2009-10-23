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

#ifndef _CFG_H_
#define _CFG_H_

#include <inttypes.h>

#define _CONFIG_MAKE_PROTOTYPE(fname,type) int config_get##fname(const char *name,type def,type *val)

int config_load (const char *fname);

_CONFIG_MAKE_PROTOTYPE(newstr,char*);
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
_CONFIG_MAKE_PROTOTYPE(newstr_nolog,char*);
_CONFIG_MAKE_PROTOTYPE(num_nolog,int);
_CONFIG_MAKE_PROTOTYPE(uint8_nolog,uint8_t);
_CONFIG_MAKE_PROTOTYPE(int8_nolog,int8_t);
_CONFIG_MAKE_PROTOTYPE(uint16_nolog,uint16_t);
_CONFIG_MAKE_PROTOTYPE(int16_nolog,int16_t);
_CONFIG_MAKE_PROTOTYPE(uint32_nolog,uint32_t);
_CONFIG_MAKE_PROTOTYPE(int32_nolog,int32_t);
_CONFIG_MAKE_PROTOTYPE(uint64_nolog,uint64_t);
_CONFIG_MAKE_PROTOTYPE(int64_nolog,int64_t);
_CONFIG_MAKE_PROTOTYPE(double_nolog,double);

#endif
