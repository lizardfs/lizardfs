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
#include <vector>

#include "common/case_sensitivity.h"

extern std::vector<std::string> gExtraArguments;

/*! \brief Returns additional command line arguments.
 *
 * Additional command line arguments can be passed using '-o extra_option' syntax.
 */
const std::vector<std::string>& main_get_extra_arguments();

/*! \brief Returns true if additional command line argument is present.
 *
 * Additional command line arguments can be passed using '-o extra_option' syntax.
 */
bool main_has_extra_argument(std::string name, CaseSensitivity mode = CaseSensitivity::kSensitive);

