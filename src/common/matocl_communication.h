#pragma once

#include "common/access_control_list.h"
#include "common/attributes.h"
#include "common/packet.h"
#include "common/serialization_macros.h"
#include "common/string_8bit.h"

// LIZ_MATOCL_FUSE_MKNOD
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseMknod, kStatusPacketVersion, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseMknod, kResponsePacketVersion, 1)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseMknod, LIZ_MATOCL_FUSE_MKNOD, kStatusPacketVersion,
		uint32_t, messageId,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseMknod, LIZ_MATOCL_FUSE_MKNOD, kResponsePacketVersion,
		uint32_t, messageId,
		uint32_t, inode,
		Attributes, attributes)

// LIZ_MATOCL_FUSE_MKDIR
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseMkdir, kStatusPacketVersion, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseMkdir, kResponsePacketVersion, 1)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseMkdir, LIZ_MATOCL_FUSE_MKDIR, kStatusPacketVersion,
		uint32_t, messageId,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseMkdir, LIZ_MATOCL_FUSE_MKDIR, kResponsePacketVersion,
		uint32_t, messageId,
		uint32_t, inode,
		Attributes, attributes)

// LIZ_MATOCL_FUSE_DELETE_ACL
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseDeleteAcl, LIZ_MATOCL_FUSE_DELETE_ACL, 0,
		uint32_t, messageId,
		uint8_t, status)

// LIZ_MATOCL_FUSE_GET_ACL
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseGetAcl, kStatusPacketVersion, 0)
LIZARDFS_DEFINE_PACKET_VERSION(matocl, fuseGetAcl, kResponsePacketVersion, 1)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseGetAcl, LIZ_MATOCL_FUSE_GET_ACL, kStatusPacketVersion,
		uint32_t, messageId,
		uint8_t, status)

LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseGetAcl, LIZ_MATOCL_FUSE_GET_ACL, kResponsePacketVersion,
		uint32_t, messageId,
		AccessControlList, acl)

// LIZ_MATOCL_FUSE_SET_ACL
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, fuseSetAcl, LIZ_MATOCL_FUSE_SET_ACL, 0,
		uint32_t, messageId,
		uint8_t, status)

// LIZ_MATOCL_IOLIMITS_CONFIG
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, iolimits_config, LIZ_MATOCL_IOLIMITS_CONFIG, 0,
		std::string, subsystem,
		std::vector<std::string>, groups,
		uint32_t, renewFrequency_us)

// LIZ_MATOCL_IOLIMIT
LIZARDFS_DEFINE_PACKET_SERIALIZATION(
		matocl, iolimit, LIZ_MATOCL_IOLIMIT, 0,
		std::string, group,
		uint64_t, limit_Bps)
