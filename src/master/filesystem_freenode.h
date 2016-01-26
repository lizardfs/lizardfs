/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare,
   2013-2015 Skytechnology sp. z o.o..

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
#include "master/filesystem_node.h"

/*! \brief Get next free inode number.
 *
 * \param ts        - current time stamp
 * \param req_inode - request inode number
 *                    >0 - we request specific inode number
 *                     0 - get any free inode number
 *
 * \return 0  - no more free inodes
 *         >0 - allocated inode number (may differ from requested if it was already taken)
 */
uint32_t fsnodes_get_next_id(uint32_t ts, uint32_t req_inode);
