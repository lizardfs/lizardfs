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

#include "fsal.h"
#include "fsal_api.h"
#include "fsal_convert.h"
#include "fsal_types.h"
#include "fsal_up.h"
#include "FSAL/fsal_commonlib.h"
#include "nfs_exports.h"
#include "pnfs_utils.h"

#include "context_wrap.h"
#include "lzfs_internal.h"

/*! \brief Clean up a DS handle
 *
 * \see fsal_api.h for more information
 */
static void lzfs_fsal_ds_handle_release(struct fsal_ds_handle *const ds_pub) {
	struct lzfs_fsal_export *lzfs_export;
	struct lzfs_fsal_ds_handle *lzfs_ds;

	lzfs_export = container_of(ds_pub->pds->mds_fsal_export, struct lzfs_fsal_export, export);
	lzfs_ds = container_of(ds_pub, struct lzfs_fsal_ds_handle, ds);

	if (lzfs_ds->file_handle != NULL) {
		liz_release(lzfs_export->lzfs_instance, lzfs_ds->file_handle);
	}

	fsal_ds_handle_fini(&lzfs_ds->ds);
	gsh_free(lzfs_ds);
}

static nfsstat4 lzfs_int_openfile(struct lzfs_fsal_export *lzfs_export,
                                  struct lzfs_fsal_ds_handle *lzfs_ds) {
	if (lzfs_ds->file_handle != NULL) {
		return NFS4_OK;
	}

	lzfs_ds->file_handle = liz_cred_open(lzfs_export->lzfs_instance, NULL, lzfs_ds->inode, O_RDWR);

	if (!lzfs_ds->file_handle) {
		return NFS4ERR_IO;
	}

	return NFS4_OK;
}

/*! \brief Read from a data-server handle.
 *
 * \see fsal_api.h for more information
 */
static nfsstat4 lzfs_fsal_ds_handle_read(struct fsal_ds_handle *const ds_hdl,
                                         struct req_op_context *const req_ctx,
                                         const stateid4 *stateid, const offset4 offset,
                                         const count4 requested_length, void *const buffer,
                                         count4 *const supplied_length, bool *const end_of_file) {
	struct lzfs_fsal_export *lzfs_export;
	struct lzfs_fsal_ds_handle *lzfs_ds;
	ssize_t nb_read;
	nfsstat4 nfs_status;

	lzfs_export = container_of(ds_hdl->pds->mds_fsal_export, struct lzfs_fsal_export, export);
	lzfs_ds = container_of(ds_hdl, struct lzfs_fsal_ds_handle, ds);

	nfs_status = lzfs_int_openfile(lzfs_export, lzfs_ds);
	if (nfs_status != NFS4_OK) {
		return nfs_status;
	}

	nb_read = liz_cred_read(lzfs_export->lzfs_instance, NULL, lzfs_ds->file_handle, offset,
	                        requested_length, buffer);

	if (nb_read < 0) {
		return lzfs_nfs4_last_err();
	}

	*supplied_length = nb_read;
	*end_of_file = (nb_read == 0);

	return NFS4_OK;
}

/*! \brief Write to a data-server handle.
 *
 * \see fsal_api.h for more information
 */
static nfsstat4 lzfs_fsal_ds_handle_write(struct fsal_ds_handle *const ds_hdl,
                                          struct req_op_context *const req_ctx,
                                          const stateid4 *stateid, const offset4 offset,
                                          const count4 write_length, const void *buffer,
                                          const stable_how4 stability_wanted,
                                          count4 *const written_length, verifier4 *const writeverf,
                                          stable_how4 *const stability_got) {
	struct lzfs_fsal_export *lzfs_export;
	struct lzfs_fsal_ds_handle *lzfs_ds;
	ssize_t nb_write;
	nfsstat4 nfs_status;

	lzfs_export = container_of(ds_hdl->pds->mds_fsal_export, struct lzfs_fsal_export, export);
	lzfs_ds = container_of(ds_hdl, struct lzfs_fsal_ds_handle, ds);

	nfs_status = lzfs_int_openfile(lzfs_export, lzfs_ds);
	if (nfs_status != NFS4_OK) {
		return nfs_status;
	}

	nb_write = liz_cred_write(lzfs_export->lzfs_instance, NULL, lzfs_ds->file_handle, offset,
	                          write_length, buffer);

	if (nb_write < 0) {
		return lzfs_nfs4_last_err();
	}

	*written_length = nb_write;
	*stability_got = stability_wanted;

	return NFS4_OK;
}

/*! \brief Commit a byte range to a DS handle.
 *
 * \see fsal_api.h for more information
 */
static nfsstat4 lzfs_fsal_ds_handle_commit(struct fsal_ds_handle *const ds_hdl,
                                           struct req_op_context *const req_ctx,
                                           const offset4 offset, const count4 count,
                                           verifier4 *const writeverf) {
	memset(*writeverf, 0, NFS4_VERIFIER_SIZE);

	LogCrit(COMPONENT_PNFS, "Commits should go to MDS\n");
	return NFS4_OK;
}

/*! \brief Read plus from a data-server handle.
 *
 * \see fsal_api.h for more information
 */
static nfsstat4 lzfs_fsal_ds_read_plus(struct fsal_ds_handle *const ds_hdl,
                                       struct req_op_context *const req_ctx,
                                       const stateid4 *stateid, const offset4 offset,
                                       const count4 requested_length, void *const buffer,
                                       const count4 supplied_length, bool *const end_of_file,
                                       struct io_info *info) {
	LogCrit(COMPONENT_PNFS, "Unimplemented DS read_plus!");
	return NFS4ERR_NOTSUPP;
}

/*! \brief Write plus to a data-server handle.
 *
 * \see fsal_api.h for more information
 */
static nfsstat4 lzfs_fsal_ds_write_plus(struct fsal_ds_handle *const ds_hdl,
                                        struct req_op_context *const req_ctx,
                                        const stateid4 *stateid, const offset4 offset,
                                        const count4 write_length, const void *buffer,
                                        const stable_how4 stability_wanted,
                                        count4 *const written_length, verifier4 *const writeverf,
                                        stable_how4 *const stability_got, struct io_info *info) {
	LogCrit(COMPONENT_PNFS, "Unimplemented DS write_plus!");
	return NFS4ERR_NOTSUPP;
}

/*! \brief Initialize FSAL specific values for DS handle
 *
 * \see fsal_api.h for more information
 */
static void lzfs_fsal_dsh_ops_init(struct fsal_dsh_ops *ops) {
	memset(ops, 0, sizeof(struct fsal_dsh_ops));

	ops->release = lzfs_fsal_ds_handle_release;
	ops->read = lzfs_fsal_ds_handle_read;
	ops->write = lzfs_fsal_ds_handle_write;
	ops->commit = lzfs_fsal_ds_handle_commit;
	ops->read_plus = lzfs_fsal_ds_read_plus;
	ops->write_plus = lzfs_fsal_ds_write_plus;
}

/*! \brief Create a FSAL data server handle from a wire handle
 *
 * \see fsal_api.h for more information
 */
static nfsstat4 lzfs_fsal_make_ds_handle(struct fsal_pnfs_ds *const pds,
                                         const struct gsh_buffdesc *const desc,
                                         struct fsal_ds_handle **const handle, int flags) {
	struct lzfs_fsal_ds_wire *dsw = (struct lzfs_fsal_ds_wire *)desc->addr;
	struct lzfs_fsal_ds_handle *lzfs_ds;

	*handle = NULL;

	if (desc->len != sizeof(struct lzfs_fsal_ds_wire))
		return NFS4ERR_BADHANDLE;
	if (dsw->inode == 0)
		return NFS4ERR_BADHANDLE;

	lzfs_ds = gsh_calloc(1, sizeof(struct lzfs_fsal_ds_handle));

	*handle = &lzfs_ds->ds;
	fsal_ds_handle_init(*handle, pds);

#if (BYTE_ORDER != BIG_ENDIAN)
	lzfs_ds->inode = bswap_32(dsw->inode);
#else
	lzfs_ds->inode = dsw->inode;
#endif

	return NFS4_OK;
}

/*! \brief Clean up a server
 *
 * \see fsal_api.h for more information
 */
static void lzfs_fsal_pds_release(struct fsal_pnfs_ds *const pds) {
	LogDebug(COMPONENT_PNFS, "pNFS DS release!");
	fsal_pnfs_ds_fini(pds);
	gsh_free(pds);
}

/*! \brief Initialize FSAL specific permissions per pNFS DS
 *
 * \see fsal_api.h for more information
 */
static nfsstat4 lzfs_fsal_pds_permissions(struct fsal_pnfs_ds *const pds, struct svc_req *req) {
	op_ctx->export_perms->set = root_op_export_set;
	op_ctx->export_perms->options = root_op_export_options;
	return NFS4_OK;
}

void lzfs_fsal_ds_handle_ops_init(struct fsal_pnfs_ds_ops *ops) {
	memset(ops, 0, sizeof(struct fsal_pnfs_ds_ops));
	ops->make_ds_handle = lzfs_fsal_make_ds_handle;
	ops->fsal_dsh_ops = lzfs_fsal_dsh_ops_init;
	ops->release = lzfs_fsal_pds_release;
	ops->permissions = lzfs_fsal_pds_permissions;
}
