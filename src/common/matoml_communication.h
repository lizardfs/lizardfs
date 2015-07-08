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

#include "common/packet.h"
#include "common/serialization_macros.h"

// LIZ_MATOML_REGISTER_SHADOW

LIZARDFS_DEFINE_PACKET_VERSION(matoml, registerShadow, kStatusPacketVersion, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matoml, registerShadow, kResponsePacketVersion, 1)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matoml, registerShadow, LIZ_MATOML_REGISTER_SHADOW, kStatusPacketVersion,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matoml, registerShadow, LIZ_MATOML_REGISTER_SHADOW, kResponsePacketVersion,
		uint32_t, version,
		uint64_t, metadataVersion)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matoml, changelogApplyError, LIZ_MATOML_CHANGELOG_APPLY_ERROR, 0,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matoml, endSession, LIZ_MATOML_END_SESSION, 0)
