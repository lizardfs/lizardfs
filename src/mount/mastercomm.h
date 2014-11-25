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
#include <vector>

#include "common/access_control_list.h"
#include "common/acl_type.h"
#include "common/attributes.h"
#include "common/packet.h"

void fs_getmasterlocation(uint8_t loc[14]);
uint32_t fs_getsrcip(void);

void fs_notify_sendremoved(uint32_t cnt,uint32_t *inodes);

void fs_statfs(uint64_t *totalspace,uint64_t *availspace,uint64_t *trashspace,uint64_t *reservedspace,uint32_t *inodes);
uint8_t fs_access(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t modemask);
uint8_t fs_lookup(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]);
uint8_t fs_getattr(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t attr[35]);
uint8_t fs_setattr(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t setmask,uint16_t attrmode,uint32_t attruid,uint32_t attrgid,uint32_t attratime,uint32_t attrmtime,uint8_t sugidclearmode,uint8_t attr[35]);
uint8_t fs_truncate(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint64_t attrlength,uint8_t attr[35]);
uint8_t fs_readlink(uint32_t inode,const uint8_t **path);
uint8_t fs_symlink(uint32_t parent,uint8_t nleng,const uint8_t *name,const uint8_t *path,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]);
uint8_t fs_mknod(uint32_t parent,uint8_t nleng,const uint8_t *name,uint8_t type,uint16_t mode,uint16_t umask,uint32_t uid,uint32_t gid,uint32_t rdev,uint32_t &inode,Attributes& attr);
uint8_t fs_mkdir(uint32_t parent,uint8_t nleng,const uint8_t *name,uint16_t mode,uint16_t umask,uint32_t uid,uint32_t gid,uint8_t copysgid,uint32_t &inode,Attributes& attr);
uint8_t fs_unlink(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid);
uint8_t fs_rmdir(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid);
uint8_t fs_rename(uint32_t parent_src,uint8_t nleng_src,const uint8_t *name_src,uint32_t parent_dst,uint8_t nleng,const uint8_t *name_dst,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]);
uint8_t fs_link(uint32_t inode_src,uint32_t parent_dst,uint8_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]);
uint8_t fs_getdir(uint32_t inode,uint32_t uid,uint32_t gid,const uint8_t **dbuff,uint32_t *dbuffsize);
uint8_t fs_getdir_plus(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t addtocache,const uint8_t **dbuff,uint32_t *dbuffsize);

uint8_t fs_opencheck(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t flags,uint8_t attr[35]);
void fs_release(uint32_t inode);

uint8_t fs_readchunk(uint32_t inode,uint32_t indx,uint64_t *length,uint64_t *chunkid,uint32_t *version,const uint8_t **csdata,uint32_t *csdatasize);
uint8_t fs_writechunk(uint32_t inode,uint32_t indx,uint64_t *length,uint64_t *chunkid,uint32_t *version,const uint8_t **csdata,uint32_t *csdatasize);
uint8_t fs_writeend(uint64_t chunkid, uint32_t inode, uint64_t length);

uint8_t fs_getxattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint8_t nleng,const uint8_t *name,uint8_t mode,const uint8_t **vbuff,uint32_t *vleng);
uint8_t fs_listxattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint8_t mode,const uint8_t **dbuff,uint32_t *dleng);
uint8_t fs_setxattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint8_t nleng,const uint8_t *name,uint32_t vleng,const uint8_t *value,uint8_t mode);
uint8_t fs_removexattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint8_t nleng,const uint8_t *name);

uint8_t fs_deletacl(uint32_t inode, uint32_t uid, uint32_t gid, AclType type);
uint8_t fs_getacl(uint32_t inode, uint32_t uid, uint32_t gid, AclType type, AccessControlList& acl);
uint8_t fs_setacl(uint32_t inode, uint32_t uid, uint32_t gid, AclType type, const AccessControlList& acl);

uint8_t fs_getreserved(const uint8_t **dbuff,uint32_t *dbuffsize);
uint8_t fs_gettrash(const uint8_t **dbuff,uint32_t *dbuffsize);
uint8_t fs_getdetachedattr(uint32_t inode,uint8_t attr[35]);
uint8_t fs_gettrashpath(uint32_t inode,const uint8_t **path);
uint8_t fs_settrashpath(uint32_t inode,const uint8_t *path);
uint8_t fs_undel(uint32_t inode);
uint8_t fs_purge(uint32_t inode);

uint8_t fs_custom(MessageBuffer& buffer);
uint8_t fs_raw_sendandreceive(MessageBuffer& buffer, PacketHeader::Type expectedType);
uint8_t fs_send_custom(MessageBuffer buffer);

// called before fork
int fs_init_master_connection(const char *bindhostname, const char *masterhostname,
		const char *masterportname, uint8_t meta, const char *info, const char *subfolder,
		const uint8_t passworddigest[16], uint8_t donotrememberpassword, uint8_t bgregister,
		unsigned retries, unsigned reportreservedperiod);
// called after fork
void fs_init_threads(uint32_t retries);
void fs_term(void);

class PacketHandler {
public:
	virtual bool handle(MessageBuffer buffer) = 0;
	virtual ~PacketHandler() {}
};

bool fs_register_packet_type_handler(PacketHeader::Type type, PacketHandler *handler);
bool fs_unregister_packet_type_handler(PacketHeader::Type type, PacketHandler *handler);
