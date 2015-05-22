/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013 Skytechnology sp. z o.o..

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

#include "common/metadataserver_list_entry.h"

uint32_t matomlserv_mloglist_size(void);
void matomlserv_mloglist_data(uint8_t *ptr);

/**
 * Returns list of shadow masters
 */
std::vector<MetadataserverListEntry> matomlserv_shadows();

void matomlserv_broadcast_logstring(uint64_t version,uint8_t *logstr,uint32_t logstrsize);
void matomlserv_broadcast_logrotate();
/*! \brief Broadcast status of metadata dump process to all interested parties.
 *
 * \param status - status to broadcast.
 */
void matomlserv_broadcast_metadata_saved(uint8_t status);
int matomlserv_init(void);
/*
 * Returns 1 if all connections to metaloggers were closed, 0 otherwise
 */
int matomlserv_canexit(void);
/*
 * Returns number of connected shadow masters
 */
uint32_t matomlserv_shadows_count();
