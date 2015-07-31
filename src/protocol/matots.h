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

#pragma once

#include "common/platform.h"

#include "protocol/packet.h"
#include "common/serialization_macros.h"

// LIZ_MATOTS_REGISTER_TAPESERVER
LIZARDFS_DEFINE_PACKET_VERSION(matots, registerTapeserver, kStatusPacketVersion, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matots, registerTapeserver, kResponsePacketVersion, 1)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matots, registerTapeserver, LIZ_MATOTS_REGISTER_TAPESERVER, kStatusPacketVersion,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matots, registerTapeserver, LIZ_MATOTS_REGISTER_TAPESERVER, kResponsePacketVersion,
		uint32_t, version)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matots, putFiles, LIZ_MATOTS_PUT_FILES, 0,
		std::vector<TapeKey>, tapeContents)
