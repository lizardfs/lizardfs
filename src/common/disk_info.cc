/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"
#include "common/disk_info.h"

void HddStatistics::add(const HddStatistics& other) {
	rbytes += other.rbytes;
	wbytes += other.wbytes;
	usecreadsum += other.usecreadsum;
	usecwritesum += other.usecwritesum;
	usecfsyncsum += other.usecfsyncsum;
	rops += other.rops;
	wops += other.wops;
	fsyncops += other.fsyncops;
	if (other.usecreadmax > usecreadmax) {
		usecreadmax = other.usecreadmax;
	}
	if (other.usecwritemax > usecwritemax) {
		usecwritemax = other.usecwritemax;
	}
	if (other.usecfsyncmax > usecfsyncmax) {
		usecfsyncmax = other.usecfsyncmax;
	}
}
