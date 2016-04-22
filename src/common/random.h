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

#include <cassert>
#include <random>

typedef std::mt19937 RandomEngine;

extern RandomEngine kRandomEngine;

int rnd_init(void);

template<typename T>
T rnd() {
	std::uniform_int_distribution<T> distribution;
	return distribution(kRandomEngine);
}

template<typename T>
T rnd_ranged(T range) {
	assert(range > 0);
	std::uniform_int_distribution<T> distribution(0, range - 1);
	return distribution(kRandomEngine);
}
