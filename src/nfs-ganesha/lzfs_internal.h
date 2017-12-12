/*
   Copyright 2017 Skytechnology sp. z o.o.

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

#include "fsal_api.h"
#include "FSAL/fsal_commonlib.h"

#include "fileinfo_cache.h"
#include "mount/client/lizardfs_c_api.h"

#define LIZARDFS_VERSION(major, minor, micro) (0x010000 * major + 0x0100 * minor + micro)
#define kDisconnectedChunkserverVersion LIZARDFS_VERSION(256, 0, 0);

struct lzfs_fsal_module {
	struct fsal_module fsal;
	fsal_staticfsinfo_t fs_info;
};

struct lzfs_fsal_handle;

struct lzfs_fsal_export {
	struct fsal_export export; /*< The public export object */

	liz_t *lzfs_instance;
	struct lzfs_fsal_handle *root; /*< The root handle */

	liz_fileinfo_cache_t *fileinfo_cache;

	bool pnfs_mds_enabled;
	bool pnfs_ds_enabled;
	uint32_t fileinfo_cache_timeout;
	uint32_t fileinfo_cache_max_size;
	liz_init_params_t lzfs_params;
};

struct lzfs_fsal_fd {
	fsal_openflags_t openflags;
	struct liz_fileinfo *fd;
};

struct lzfs_fsal_state_fd {
	struct state_t state;
	struct lzfs_fsal_fd lzfs_fd;
};

struct lzfs_fsal_key {
	uint16_t module_id;
	uint16_t export_id;
	liz_inode_t inode;
};

struct lzfs_fsal_handle {
	struct fsal_obj_handle handle; /*< The public handle */
	struct lzfs_fsal_fd fd;
	liz_inode_t inode;
	struct lzfs_fsal_key unique_key;
	struct lzfs_fsal_export *export;
	struct fsal_share share;
};

struct lzfs_fsal_ds_wire {
	uint32_t inode;
};

struct lzfs_fsal_ds_handle {
	struct fsal_ds_handle ds;
	uint32_t inode;
	liz_fileinfo_entry_t *cache_handle;
};

#define LZFS_LEASE_TIME 10
#define LZFS_SUPPORTED_ATTRS                                                                    \
	(ATTR_TYPE | ATTR_SIZE | ATTR_FSID | ATTR_FILEID | ATTR_MODE | ATTR_NUMLINKS | ATTR_OWNER | \
	 ATTR_GROUP | ATTR_ATIME | ATTR_CTIME | ATTR_MTIME | ATTR_CHGTIME | ATTR_CHANGE |           \
	 ATTR_SPACEUSED | ATTR_RAWDEV | ATTR_ACL)

#define LZFS_BIGGEST_STRIPE_COUNT 4096
#define LZFS_STD_CHUNK_PART_TYPE 0
#define LZFS_EXPECTED_BACKUP_DS_COUNT 3
#define TCP_PROTO_NUMBER 6

fsal_status_t lizardfs2fsal_error(liz_err_t err);
fsal_status_t lzfs_fsal_last_err();
liz_context_t *lzfs_fsal_create_context(liz_t *instance, struct user_cred *cred);
fsal_staticfsinfo_t *lzfs_fsal_staticinfo(struct fsal_module *module_hdl);
void lzfs_fsal_export_ops_init(struct export_ops *ops);
void lzfs_fsal_handle_ops_init(struct lzfs_fsal_export *lzfs_export, struct fsal_obj_ops *ops);
void lzfs_fsal_handle_ops_pnfs(struct fsal_obj_ops *ops);
void lzfs_fsal_export_ops_pnfs(struct export_ops *ops);
void lzfs_fsal_ops_pnfs(struct fsal_ops *ops);
struct lzfs_fsal_handle *lzfs_fsal_new_handle(const struct stat *attr,
                                              struct lzfs_fsal_export *lzfs_export);
void lzfs_fsal_delete_handle(struct lzfs_fsal_handle *obj);
void lzfs_fsal_ds_handle_ops_init(struct fsal_pnfs_ds_ops *ops);
nfsstat4 lzfs_nfs4_last_err();
fsal_status_t lzfs_int_getacl(struct lzfs_fsal_export *lzfs_export, uint32_t inode, uint32_t owner,
                              fsal_acl_t **fsal_acl);
fsal_status_t lzfs_int_setacl(struct lzfs_fsal_export *lzfs_export, uint32_t inode,
                              const fsal_acl_t *fsal_acl);
