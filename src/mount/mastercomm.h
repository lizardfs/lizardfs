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
#include <vector>

#include "common/access_control_list.h"
#include "common/acl_type.h"
#include "common/attributes.h"
#include "common/chunk_type_with_address.h"
#include "mount/group_cache.h"
#include "mount/lizard_client.h"
#include "protocol/packet.h"
#include "protocol/lock_info.h"
#include "protocol/directory_entry.h"
#include "protocol/named_inode_entry.h"

void fs_getmasterlocation(uint8_t loc[14]);
uint32_t fs_getsrcip(void);

void fs_notify_sendremoved(uint32_t cnt,uint32_t *inodes);

void fs_statfs(uint64_t *totalspace,uint64_t *availspace,uint64_t *trashspace,uint64_t *reservedspace,uint32_t *inodes);
uint8_t fs_access(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t modemask);
uint8_t fs_lookup(uint32_t parent, const std::string &path, uint32_t uid, uint32_t gid, uint32_t *inode, Attributes &attr);
uint8_t fs_getattr(uint32_t inode, uint32_t uid, uint32_t gid, Attributes &attr);
uint8_t fs_setattr(uint32_t inode, uint32_t uid, uint32_t gid, uint8_t setmask, uint16_t attrmode, uint32_t attruid, uint32_t attrgid, uint32_t attratime, uint32_t attrmtime, uint8_t sugidclearmode, Attributes &attr);
uint8_t fs_truncate(uint32_t inode, bool opened, uint32_t uid, uint32_t gid, uint64_t length,
		bool& clientPerforms, Attributes& attr, uint64_t& oldLength, uint32_t& lockId);
uint8_t fs_truncateend(uint32_t inode, uint32_t uid, uint32_t gid, uint64_t length, uint32_t lockId,
		Attributes& attr);
uint8_t fs_readlink(uint32_t inode,const uint8_t **path);
uint8_t fs_symlink(uint32_t parent, uint8_t nleng, const uint8_t *name, const uint8_t *path, uint32_t uid, uint32_t gid, uint32_t *inode, Attributes &attr);
uint8_t fs_mknod(uint32_t parent, uint8_t nleng, const uint8_t *name, uint8_t type, uint16_t mode, uint16_t umask, uint32_t uid, uint32_t gid, uint32_t rdev, uint32_t &inode, Attributes& attr);
uint8_t fs_mkdir(uint32_t parent, uint8_t nleng, const uint8_t *name, uint16_t mode, uint16_t umask, uint32_t uid, uint32_t gid, uint8_t copysgid, uint32_t &inode, Attributes& attr);
uint8_t fs_unlink(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid);
uint8_t fs_rmdir(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid);
uint8_t fs_rename(uint32_t parent_src, uint8_t nleng_src, const uint8_t *name_src, uint32_t parent_dst, uint8_t nleng, const uint8_t *name_dst, uint32_t uid, uint32_t gid, uint32_t *inode, Attributes &attr);
uint8_t fs_link(uint32_t inode_src, uint32_t parent_dst, uint8_t nleng_dst, const uint8_t *name_dst, uint32_t uid, uint32_t gid, uint32_t *inode, Attributes &attr);
uint8_t fs_getdir(uint32_t inode,uint32_t uid,uint32_t gid,const uint8_t **dbuff,uint32_t *dbuffsize);
uint8_t fs_getdir_plus(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t addtocache,const uint8_t **dbuff,uint32_t *dbuffsize);
uint8_t fs_getdir(uint32_t inode, uint32_t uid, uint32_t gid, uint64_t first_entry, uint64_t max_entries, std::vector<DirectoryEntry> &dir_entries);

uint8_t fs_opencheck(uint32_t inode, uint32_t uid, uint32_t gid, uint8_t flags, Attributes &attr);
uint8_t fs_update_credentials(uint32_t key, const GroupCache::Groups &gids);
void fs_release(uint32_t inode);

uint8_t fs_readchunk(uint32_t inode,uint32_t indx,uint64_t *length,uint64_t *chunkid,uint32_t *version,const uint8_t **csdata,uint32_t *csdatasize);
uint8_t fs_lizreadchunk(std::vector<ChunkTypeWithAddress> &serverList, uint64_t &chunkId,
		uint32_t &chunkVersion, uint64_t &fileLength, uint32_t inode, uint32_t index);
uint8_t fs_writechunk(uint32_t inode,uint32_t indx,uint64_t *length,uint64_t *chunkid,uint32_t *version,const uint8_t **csdata,uint32_t *csdatasize);
uint8_t fs_lizwritechunk(uint32_t inode, uint32_t chunkIndex, uint32_t &lockId,
		uint64_t &fileLength, uint64_t &chunkId, uint32_t &chunkVersion,
		std::vector<ChunkTypeWithAddress> &chunkservers);
uint8_t fs_lizwriteend(uint64_t chunkId, uint32_t lockId, uint32_t inode, uint64_t length);
uint8_t fs_writeend(uint64_t chunkid, uint32_t inode, uint64_t length);

uint8_t fs_getxattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint8_t nleng,const uint8_t *name,uint8_t mode,const uint8_t **vbuff,uint32_t *vleng);
uint8_t fs_listxattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint8_t mode,const uint8_t **dbuff,uint32_t *dleng);
uint8_t fs_setxattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint8_t nleng,const uint8_t *name,uint32_t vleng,const uint8_t *value,uint8_t mode);
uint8_t fs_removexattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint8_t nleng,const uint8_t *name);

uint8_t fs_deletacl(uint32_t inode, uint32_t uid, uint32_t gid, AclType type);
uint8_t fs_getacl(uint32_t inode, uint32_t uid, uint32_t gid, RichACL& acl, uint32_t &owner_id);
uint8_t fs_setacl(uint32_t inode, uint32_t uid, uint32_t gid, const RichACL& acl);
uint8_t fs_setacl(uint32_t inode, uint32_t uid, uint32_t gid, AclType type, const AccessControlList& acl);

uint8_t fs_getreserved(const uint8_t **dbuff,uint32_t *dbuffsize);
uint8_t fs_getreserved(LizardClient::NamedInodeOffset off, LizardClient::NamedInodeOffset max_entries,
	               std::vector<NamedInodeEntry> &entries);
uint8_t fs_gettrash(const uint8_t **dbuff,uint32_t *dbuffsize);
uint8_t fs_gettrash(LizardClient::NamedInodeOffset off, LizardClient::NamedInodeOffset max_entries,
	            std::vector<NamedInodeEntry> &entries);
uint8_t fs_getdetachedattr(uint32_t inode, Attributes &attr);
uint8_t fs_gettrashpath(uint32_t inode,const uint8_t **path);
uint8_t fs_settrashpath(uint32_t inode,const uint8_t *path);
uint8_t fs_undel(uint32_t inode);
uint8_t fs_purge(uint32_t inode);

uint8_t fs_getlk(uint32_t inode, uint64_t owner, lzfs_locks::FlockWrapper &lock);
uint8_t fs_setlk_send(uint32_t inode, uint64_t owner, uint32_t reqid, const lzfs_locks::FlockWrapper &lock);
uint8_t fs_setlk_recv();
uint8_t fs_flock_send(uint32_t inode, uint64_t owner, uint32_t reqid, uint16_t op);
uint8_t fs_flock_recv();
void fs_flock_interrupt(const lzfs_locks::InterruptData &data);
void fs_setlk_interrupt(const lzfs_locks::InterruptData &data);

uint8_t fs_makesnapshot(uint32_t src_inode, uint32_t dst_parent, const std::string &dst_name,
	                uint32_t uid, uint32_t gid, uint8_t can_overwrite, uint32_t &job_id);
uint8_t fs_getgoal(uint32_t inode, std::string &goal);
uint8_t fs_setgoal(uint32_t inode, uint32_t uid, const std::string &goal_name, uint8_t smode);

uint8_t fs_custom(MessageBuffer& buffer);
uint8_t fs_raw_sendandreceive(MessageBuffer& buffer, PacketHeader::Type expectedType);
uint8_t fs_send_custom(MessageBuffer buffer);
uint8_t fs_getchunksinfo(uint32_t uid, uint32_t gid, uint32_t inode, uint32_t chunk_index,
		uint32_t chunk_count, std::vector<ChunkWithAddressAndLabel> &chunks);
uint8_t fs_getchunkservers(std::vector<ChunkserverListEntry> &chunkservers);

// called after fork
int fs_init_master_connection(LizardClient::FsInitParams &params);
void fs_init_threads(uint32_t retries);
void fs_term(void);

class PacketHandler {
public:
	virtual bool handle(MessageBuffer buffer) = 0;
	virtual ~PacketHandler() {}
};

bool fs_register_packet_type_handler(PacketHeader::Type type, PacketHandler *handler);
bool fs_unregister_packet_type_handler(PacketHeader::Type type, PacketHandler *handler);
