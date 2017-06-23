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

#include "mount/client/lizardfs_c_api.h"

struct lzfs_fsal_module {
	struct fsal_module fsal;
	fsal_staticfsinfo_t fs_info;
};

struct lzfs_fsal_handle;

struct lzfs_fsal_export {
	struct fsal_export export; /*< The public export object */

	liz_t *lzfs_instance;
	struct lzfs_fsal_handle *root; /*< The root handle */

	char *lzfs_hostname;
	char *lzfs_port;
};

struct lzfs_fsal_fd {
	fsal_openflags_t openflags;
	struct liz_fileinfo *fd;
};

struct lzfs_fsal_state_fd {
	struct state_t state;
	struct lzfs_fsal_fd lzfs_fd;
};

struct lzfs_fsal_handle {
	struct fsal_obj_handle handle; /*< The public handle */
	struct lzfs_fsal_fd fd;
	liz_inode_t inode;
	struct lzfs_fsal_export *export;
	struct fsal_share share;
};

#define LZFS_SUPPORTED_ATTRS                                                                    \
	(ATTR_TYPE | ATTR_SIZE | ATTR_FSID | ATTR_FILEID | ATTR_MODE | ATTR_NUMLINKS | ATTR_OWNER | \
	 ATTR_GROUP | ATTR_ATIME | ATTR_CTIME | ATTR_MTIME | ATTR_CHGTIME | ATTR_CHANGE |           \
	 ATTR_SPACEUSED | ATTR_RAWDEV)

#define LZFS_LEASE_TIME 10

fsal_status_t lizardfs2fsal_error(liz_err_t err);
fsal_status_t lzfs_fsal_last_err();
liz_context_t *lzfs_fsal_create_context(liz_t *instance, struct user_cred *cred);
fsal_staticfsinfo_t *lzfs_fsal_staticinfo(struct fsal_module *module_hdl);
void lzfs_fsal_export_ops_init(struct export_ops *ops);
void lzfs_fsal_handle_ops_init(struct fsal_obj_ops *ops);
struct lzfs_fsal_handle *lzfs_fsal_new_handle(const struct stat *attr,
                                              struct lzfs_fsal_export *lzfs_export);
void lzfs_fsal_delete_handle(struct lzfs_fsal_handle *obj);
