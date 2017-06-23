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

#include <limits.h>

#include "fsal.h"
#include "fsal_api.h"
#include "fsal_types.h"
#include "FSAL/fsal_commonlib.h"
#include "FSAL/fsal_init.h"

#include "common/special_inode_defs.h"
#include "context_wrap.h"
#include "lzfs_internal.h"
#include "protocol/MFSCommunication.h"

static struct lzfs_fsal_module gLizardFSM;
static const char *gModuleName = "LizardFS";

static fsal_staticfsinfo_t default_lizardfs_info = {
    .maxfilesize = UINT64_MAX,
    .maxlink = _POSIX_LINK_MAX,
    .maxnamelen = MFS_NAME_MAX,
    .maxpathlen = MAXPATHLEN,
    .no_trunc = true,
    .chown_restricted = false,
    .case_insensitive = false,
    .case_preserving = true,
    .link_support = true,
    .symlink_support = true,
    .named_attr = true,
    .unique_handles = true,
    .lease_time = {LZFS_LEASE_TIME, 0},
    .acl_support = 0,
    .cansettime = true,
    .homogenous = true,
    .supported_attrs = LZFS_SUPPORTED_ATTRS,
    .maxread = FSAL_MAXIOSIZE,
    .maxwrite = FSAL_MAXIOSIZE,
    .umask = 0,
    .auth_exportpath_xdev = false,
    .xattr_access_rights = 0,
    .share_support = false,
    .share_support_owner = false,
    .pnfs_mds = false,
    .pnfs_ds = false,
    .fsal_trace = false,
    .fsal_grace = false,
    .link_supports_permission_checks = true,
};

static struct config_item lzfs_fsal_items[] = {
    CONF_ITEM_MODE("umask", 0, fsal_staticfsinfo_t, umask),
    CONF_ITEM_MODE("xattr_access_rights", 0, fsal_staticfsinfo_t, xattr_access_rights),
    CONF_ITEM_BOOL("link_support", true, fsal_staticfsinfo_t, link_support),
    CONF_ITEM_BOOL("symlink_support", true, fsal_staticfsinfo_t, symlink_support),
    CONF_ITEM_BOOL("cansettime", true, fsal_staticfsinfo_t, cansettime),
    CONF_ITEM_BOOL("auth_xdev_export", false, fsal_staticfsinfo_t, auth_exportpath_xdev),
    CONF_ITEM_BOOL("fsal_trace", false, fsal_staticfsinfo_t, fsal_trace),
    CONF_ITEM_BOOL("fsal_grace", false, fsal_staticfsinfo_t, fsal_grace),
    CONFIG_EOL};

static struct config_block lzfs_fsal_param_block = {
    .dbus_interface_name = "org.ganesha.nfsd.config.fsal.lizardfs",
    .blk_desc.name = "LizardFS",
    .blk_desc.type = CONFIG_BLOCK,
    .blk_desc.u.blk.init = noop_conf_init,
    .blk_desc.u.blk.params = lzfs_fsal_items,
    .blk_desc.u.blk.commit = noop_conf_commit};

static struct config_item lzfs_fsal_export_params[] = {
    CONF_ITEM_NOOP("name"),
    CONF_MAND_STR("hostname", 1, MAXPATHLEN, NULL, lzfs_fsal_export, lzfs_hostname),
    CONF_ITEM_STR("port", 1, MAXPATHLEN, "9421", lzfs_fsal_export, lzfs_port), CONFIG_EOL};

static struct config_block lzfs_fsal_export_param_block = {
    .dbus_interface_name = "org.ganesha.nfsd.config.fsal.lizardfs-export%d",
    .blk_desc.name = "FSAL",
    .blk_desc.type = CONFIG_BLOCK,
    .blk_desc.u.blk.init = noop_conf_init,
    .blk_desc.u.blk.params = lzfs_fsal_export_params,
    .blk_desc.u.blk.commit = noop_conf_commit};

static fsal_status_t lzfs_fsal_create_export(struct fsal_module *fsal_hdl, void *parse_node,
                                             struct config_error_type *err_type,
                                             const struct fsal_up_vector *up_ops) {
	struct lzfs_fsal_export *lzfs_export;
	fsal_status_t status = fsalstat(ERR_FSAL_NO_ERROR, 0);
	int rc;

	lzfs_export = gsh_calloc(1, sizeof(struct lzfs_fsal_export));

	fsal_export_init(&lzfs_export->export);
	lzfs_fsal_export_ops_init(&lzfs_export->export.exp_ops);

	// parse params for this export
	if (parse_node) {
		rc = load_config_from_node(parse_node, &lzfs_fsal_export_param_block, lzfs_export, true,
		                           err_type);
		if (rc != 0) {
			LogCrit(COMPONENT_FSAL, "Failed to parse export configuration for %s",
			        op_ctx->ctx_export->fullpath);

			status = fsalstat(ERR_FSAL_INVAL, 0);
			goto error;
		}
	}

	// FIXME(haze): Add support for all paths.
	if (strcmp(op_ctx->ctx_export->fullpath, "/") != 0) {
		LogCrit(COMPONENT_FSAL, "Only '/' export path is supported");
		return fsalstat(ERR_FSAL_INVAL, 0);
	}

	// connect to LizardFS
	lzfs_export->lzfs_instance =
	    liz_init(lzfs_export->lzfs_hostname, lzfs_export->lzfs_port, op_ctx->ctx_export->fullpath);

	if (lzfs_export->lzfs_instance == NULL) {
		LogCrit(COMPONENT_FSAL, "Unable to mount LizardFS cluster for %s.",
		        op_ctx->ctx_export->fullpath);
		status = fsalstat(ERR_FSAL_SERVERFAULT, 0);
		goto error;
	}

	if (fsal_attach_export(fsal_hdl, &lzfs_export->export.exports) != 0) {
		LogCrit(COMPONENT_FSAL, "Unable to attach export for %s.", op_ctx->ctx_export->fullpath);
		status = fsalstat(ERR_FSAL_SERVERFAULT, 0);
		goto error;
	}

	lzfs_export->export.fsal = fsal_hdl;
	lzfs_export->export.up_ops = up_ops;

	// get attributes for root inode
	liz_attr_reply_t ret;
	rc = liz_cred_getattr(lzfs_export->lzfs_instance, op_ctx->creds, SPECIAL_INODE_ROOT, &ret);
	if (rc < 0) {
		status = lzfs_fsal_last_err();
		goto error;
	}

	lzfs_export->root = lzfs_fsal_new_handle(&ret.attr, lzfs_export);
	op_ctx->fsal_export = &lzfs_export->export;

	LogDebug(COMPONENT_FSAL, "LizardFS module export %s.", op_ctx->ctx_export->fullpath);

	return fsalstat(ERR_FSAL_NO_ERROR, 0);
error:
	if (lzfs_export) {
		if (lzfs_export->lzfs_instance) {
			liz_destroy(lzfs_export->lzfs_instance);
		}
		gsh_free(lzfs_export);
	}

	return status;
}

static fsal_status_t lzfs_fsal_init_config(struct fsal_module *module_in,
                                           config_file_t config_struct,
                                           struct config_error_type *err_type) {
	struct lzfs_fsal_module *lzfs_module;

	lzfs_module = container_of(module_in, struct lzfs_fsal_module, fsal);

	LogDebug(COMPONENT_FSAL, "LizardFS module setup.");

	lzfs_module->fs_info = default_lizardfs_info;
	(void)load_config_from_parse(config_struct, &lzfs_fsal_param_block, &lzfs_module->fs_info, true,
	                             err_type);
	if (!config_error_is_harmless(err_type))
		return fsalstat(ERR_FSAL_INVAL, 0);

	display_fsinfo(&lzfs_module->fs_info);

	return fsalstat(ERR_FSAL_NO_ERROR, 0);
}

bool lzfs_fsal_support_ex(struct fsal_obj_handle *obj) {
	return true;
}

MODULE_INIT void init(void) {
	struct fsal_module *lzfs_module = &gLizardFSM.fsal;

	LogDebug(COMPONENT_FSAL, "LizardFS module registering.");

	memset(lzfs_module, 0, sizeof(*lzfs_module));
	if (register_fsal(lzfs_module, gModuleName, FSAL_MAJOR_VERSION, FSAL_MINOR_VERSION,
	                  FSAL_ID_EXPERIMENTAL) != 0) {
		LogCrit(COMPONENT_FSAL, "LizardFS module failed to register.");
	}

	lzfs_module->m_ops.create_export = lzfs_fsal_create_export;
	lzfs_module->m_ops.init_config = lzfs_fsal_init_config;
	lzfs_module->m_ops.support_ex = lzfs_fsal_support_ex;
}

MODULE_FINI void finish(void) {
	LogDebug(COMPONENT_FSAL, "LizardFS module finishing.");

	if (unregister_fsal(&gLizardFSM.fsal) != 0) {
		LogCrit(COMPONENT_FSAL, "Unable to unload LizardFS FSAL.  Dying with extreme prejudice.");
		abort();
	}
}
