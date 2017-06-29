/*
 * Copyright 2017 Skytechnology sp. z o.o..
 * Author: Piotr Sarna <sarna@skytechnology.pl>
 *
 * LizardFS C API
 *
 * This library can be used to communicate with LizardFS metadata and data
 * servers from C/C++ code.
 *
 * Compile with -llizardfs-client and LizardFS C/C++ library installed.
 */

#include <fcntl.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/uio.h>

#ifndef __LIZARDFS_C_API_H
#define __LIZARDFS_C_API_H

#ifdef __cplusplus
extern "C" {
#endif

#define LIZARDFS_MAX_GOAL_NAME 64

typedef uint32_t liz_inode_t;
typedef int liz_err_t;
struct liz;
typedef struct liz liz_t;
struct liz_fileinfo;
typedef struct liz_fileinfo liz_fileinfo_t;
struct liz_context;
typedef struct liz_context liz_context_t;

#define LIZ_SET_ATTR_MODE      (1 << 0)
#define LIZ_SET_ATTR_UID       (1 << 1)
#define LIZ_SET_ATTR_GID       (1 << 2)
#define LIZ_SET_ATTR_SIZE      (1 << 3)
#define LIZ_SET_ATTR_ATIME     (1 << 4)
#define LIZ_SET_ATTR_MTIME     (1 << 5)
#define LIZ_SET_ATTR_ATIME_NOW (1 << 7)
#define LIZ_SET_ATTR_MTIME_NOW (1 << 8)

enum liz_special_ino {
	LIZARDFS_INODE_ERROR = 0,
	LIZARDFS_INODE_ROOT = 1,
};

enum liz_setxattr_mode {
	XATTR_SMODE_CREATE_OR_REPLACE = 0,
	XATTR_SMODE_CREATE_ONLY       = 1,
	XATTR_SMODE_REPLACE_ONLY      = 2,
	XATTR_SMODE_REMOVE            = 3,
};

/* Basic attributes of a file */
typedef struct liz_entry {
	liz_inode_t ino;
	unsigned long generation;
	struct stat attr;
	double attr_timeout;
	double entry_timeout;
} liz_entry_t;

/* Result of setattr/getattr operations */
typedef struct liz_attr_reply {
	struct stat attr;
	double attr_timeout;
} liz_attr_reply_t;

/* Basic attributes of a directory */
typedef struct liz_direntry {
	char *name;
	struct stat attr;
	off_t next_entry_offset;
} liz_direntry_t;

typedef struct liz_namedinode_entry {
	liz_inode_t ino;
	char *name;
} liz_namedinode_entry_t;

/* Result of getxattr, setxattr and listattr operations */
typedef struct liz_xattr_reply {
	uint32_t value_length;
	uint8_t *value_buffer;
} liz_xattr_reply_t;

/* Result of statfs operation
 * total_space - total space
 * avail_space - available space
 * trash_space - space occupied by trash files
 * reserved_space - space occupied by reserved files
 * inodes - number of inodes
 */
typedef struct liz_stat {
	uint64_t total_space;
	uint64_t avail_space;
	uint64_t trash_space;
	uint64_t reserved_space;
	uint32_t inodes;
} liz_stat_t;

/*!
 * \brief Create a context for LizardFS operations
 *  Flavor 1: create default context with current uid/gid/pid
 *  Flavor 2: create context with custom uid/gid/pid
 *
 *  \warning Creating context with secondary groups involves calling liz_update_groups
 *  on a created context. It is the case because metadata server needs to be notified
 *  that new group set was created. If secondary groups are registered by calling
 *  liz_update_groups(ctx, instance), context is bound to instance it was registered with
 *  and should not be used with other instances.
 */
liz_context_t *liz_create_context();
liz_context_t *liz_create_user_context(uid_t uid, gid_t gid, pid_t pid, mode_t umask);

/*!
 * \brief Returns last error code set by specific calls (see below)
 */
liz_err_t liz_last_err();

/*!
 * \brief Converts native LizardFS error code to POSIX error code.
 */
int liz_error_conv(liz_err_t lizardfs_error_code);

/*!
 * \brief Returns human-readable description of LizardFS error code
 */
const char *liz_error_string(liz_err_t lizardfs_error_code);

/*!
 * \brief Destroy a context for LizardFS operations
 */
void liz_destroy_context(liz_context_t *ctx);

/*!
 * \brief Initialize a connection with master server
 * \param host master server connection host
 * \param port master server connection port
 * \param mountpoint a human-readable name for 'mountpoint' created by a connection
 * \return a LizardFS client instance, nullptr if connection is impossible,
 *  sets last error code (check with liz_last_err())
 */
liz_t *liz_init(const char *host, const char *port, const char *mountpoint);

/*!
 * \brief Update secondary group information in context
 * \param instance instance returned from liz_init
 * \param ctx context to be updated
 * \param gids array of new group ids to be set
 * \param gid_num length of gids array
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_update_groups(liz_t *instance, liz_context_t *ctx, gid_t *gids, int gid_num);

/*! \brief Find inode in parent directory by name
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param parent parent inode
 * \param path name to be looked up
 * \param entry structure to be filled with lookup result
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_lookup(liz_t *instance, liz_context_t *ctx, liz_inode_t parent, const char *path,
	       struct liz_entry *entry);

/*! \brief Create a file with given parent and name
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param parent parent inode
 * \param path name to be looked up
 * \param mode file permissions
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_mknod(liz_t *instance, liz_context_t *ctx, liz_inode_t parent, const char *path,
	      mode_t mode, struct liz_entry *entry);

/*! \brief Open a file by inode
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param inode inode of a file
 * \param flags open flags
 * \return fileinfo descriptor of an open file,
 *  if failed - nullptr and sets last error code (check with liz_last_err())
 */
liz_fileinfo_t *liz_open(liz_t *instance, liz_context_t *ctx, liz_inode_t inode, int flags);

/*! \brief Read bytes from open file
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param fileinfo descriptor of an open file
 * \param offset read offset
 * \param size read size
 * \param buffer buffer to be read to
 * \return number of bytes read on success,
 *  -1 if failed and sets last error code (check with liz_last_err())
 */
ssize_t liz_read(liz_t *instance, liz_context_t *ctx, liz_fileinfo_t *fileinfo, off_t offset,
	         size_t size, char *buffer);
ssize_t liz_readv(liz_t *instance, liz_context_t *ctx, liz_fileinfo_t *fileinfo, off_t offset,
	          size_t size, const struct iovec *iov, int iovcnt);

/*! \brief Write bytes to open file
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param fileinfo descriptor of an open file
 * \param offset write offset
 * \param size write size
 * \param buffer buffer to be written from
 * \return number of bytes written on success,
 *  -1 if failed and sets last error code (check with liz_last_err())
 */
ssize_t liz_write(liz_t *instance, liz_context_t *ctx, liz_fileinfo_t *fileinfo,
	          off_t offset, size_t size, const char *buffer);

/*! \brief Release a previously open file
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param fileinfo descriptor of an open file
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_release(liz_t *instance, liz_context_t *ctx, liz_fileinfo_t *fileinfo);

/*! \brief Flush data written to an open file
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param fileinfo descriptor of an open file
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_flush(liz_t *instance, liz_context_t *ctx, liz_fileinfo_t *fileinfo);

/*! \brief Get attributes by inode
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param inode inode of a file
 * \param reply structure to be filled with getattr result
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_getattr(liz_t *instance, liz_context_t *ctx, liz_inode_t inode,
	        struct liz_attr_reply *reply);

/*! \brief End a connection with master server
 * \param instance instance returned from liz_init
 */
void liz_destroy(liz_t *instance);

/*! \brief Open a directory
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param inode inode of a directory
 * \return fileinfo descriptor on success, nullptr if failed,
 *         sets last error code (check with liz_last_err())
 */
struct liz_fileinfo *liz_opendir(liz_t *instance, liz_context_t *ctx, liz_inode_t inode);

/*! \brief Read directory entries
 * \param instance instance returned from liz_init
 * \param fileinfo descriptor of an open directory
 * \param offset directory entry offset
 * \param buf buffer to be filled with readdir data
 * \param max_entries max number of entries to be returned
 * \param num_entries upon success set to number of entries returned in buf
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_readdir(liz_t *instance, liz_context_t *ctx, struct liz_fileinfo *fileinfo, off_t offset,
	                         size_t max_entries, struct liz_direntry *buf, size_t *num_entries);

/*! \brief Destroy dir entries placed in an array
 * \param buf buf argument to previous successful call to liz_readdir
 * \param num_entries positive *num_entries value after respective liz_readdir() call
 */
void liz_destroy_direntry(struct liz_direntry *buf, size_t num_entries);

/*! \brief Release a directory
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param fileinfo descriptor of an open directory
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_releasedir(liz_t *instance, liz_context_t *ctx, struct liz_fileinfo *fileinfo);

/*! \brief Get reserved file inodes and names
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param offset 0-based index of the first wanted entry
 * \param max_entries maximum number of entries to retrieve
 * \param out_entries array entries are placed in
 * \param num_entries number of entries placed in out_entries
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 * \post liz_free_namedinode_entries(out_entries, result) must be called
 *       if (returned 0 && num_entries > 0) to dispose of returned entries
 */
int liz_readreserved(liz_t *instance, liz_context_t *ctx, uint32_t offset, uint32_t max_entries,
                     liz_namedinode_entry_t *out_entries, uint32_t *num_entries);

/*! \brief Get trash file inodes and names
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param offset 0-based index of the first wanted entry
 * \param max_entries maximum number of entries to retrieve
 * \param out_entries array entries are placed in
 * \param num_entries number of entries placed in out_entries
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 * \post liz_free_namedinode_entry(out_entries, result) must be called
 *       if (returned 0 && num_entries > 0) to dispose of returned entries
 */
int liz_readtrash(liz_t *instance, liz_context_t *ctx, uint32_t offset, uint32_t max_entries,
                  liz_namedinode_entry_t *out_entries, uint32_t *num_entries);

/*! \brief Destroy named inode entries placed in an array
 * \param entries out_entries argument to previous call to either liz_readreserved or liz_readtrash
 * \param num_entries positive number of entries returned by the respective call
 */
void liz_free_namedinode_entries(struct liz_namedinode_entry *entries, uint32_t num_entries);

/*! \brief Create a directory
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param parent parent directory inode
 * \param name directory name
 * \param mode directory attributes
 * \param out_entry entry to be filled with new directory data
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_mkdir(liz_t *instance, liz_context_t *ctx, liz_inode_t parent, const char *name,
		mode_t mode, struct liz_entry *out_entry);

/*! \brief Remove a directory
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param parent parent directory inode
 * \param name directory name
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_rmdir(liz_t *instance, liz_context_t *ctx, liz_inode_t parent, const char *name);

/*! \brief Make a snapshot of a file
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param inode inode of a file
 * \param dst_parent inode of a new parent directory for a snapshot
 * \param dst_name name of a newly created snapshot
 * \param can_overwrite if true, snapshot creation will be able to overwrite existing files
 * \param job_id id of makesnapshot task, can be used to cancel it, can be NULL
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_makesnapshot(liz_t *instance, liz_context_t *ctx, liz_inode_t inode, liz_inode_t dst_parent,
	             const char *dst_name, int can_overwrite, uint32_t *job_id);

/*! \brief Get file goal
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param inode inode of a file
 * \param goal_name buffer to be filled with goal, must be at least LIZARDFS_MAX_GOAL_NAME long
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_getgoal(liz_t *instance, liz_context_t *ctx, liz_inode_t inode, char *goal_name);

/*! \brief Set file goal
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param inode inode of a file
 * \param goal_name goal name to be set
 * \param is_recursive if true, operation will apply to all subdirectories and files within them
 * \param job_id id of setgoal task, can be used to cancel it, can be NULL
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_setgoal(liz_t *instance, liz_context_t *ctx, liz_inode_t inode, const char *goal_name,
	        int is_recursive);

/*! \brief Unlink a file
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param parent parent directory inode
 * \param name file name
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_unlink(liz_t *instance, liz_context_t *ctx, liz_inode_t parent, const char *name);

/*! \brief Restore file from trash
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param inode inode of the file
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_undel(liz_t *instance, liz_context_t *ctx, liz_inode_t inode);

/*! \brief Set file attributes
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param inode inode of a file
 * \param stbuf attributes to be set
 * \param to_set flag which attributes should be set
 * \param reply returned value
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_setattr(liz_t *instance, liz_context_t *ctx, liz_inode_t inode, struct stat *stbuf,
	        int to_set, struct liz_attr_reply *reply);

/*! \brief Synchronize file data
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param fileinfo descriptor of an open file
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_fsync(liz_t *instance, liz_context_t *ctx, struct liz_fileinfo *fileinfo);

/*! \brief Rename a file
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param parent current parent of a file to be moved
 * \param name name of a file to be moved
 * \param new_parent inode of a new directory
 * \param new_name new name of a file to be moved
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_rename(liz_t *instance, liz_context_t *ctx, liz_inode_t parent, const char *name,
	       liz_inode_t new_parent, const char *new_name);

/*! \brief Retrieve file system statistics
 * \param instance instance returned from liz_init
 * \param buf structure to be filled with file system statistics
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_statfs(liz_t *instance, liz_stat_t *buf);

/*! \brief Set extended attribute of a file
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param ino inode of a file
 * \param name attribute name
 * \param value attribute value
 * \param size size of attribute value
 * \param mode one of enum liz_setxattr_mode values
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_setxattr(liz_t *instance, liz_context_t *ctx, liz_inode_t ino, const char *name,
	         const uint8_t *value, size_t size, enum liz_setxattr_mode mode);

/*! \brief Get extended attribute of a file
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param ino inode of a file
 * \param name attribute name
 * \param size size of the provided buffer
 * \param out_size filled with actual size of xattr value
 * \param buf buffer to be filled with xattr value
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_getxattr(liz_t *instance, liz_context_t *ctx, liz_inode_t ino, const char *name,
	         size_t size, size_t *out_size, uint8_t *buf);

/*! \brief Get extended attributes list of a file
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param ino inode of a file
 * \param size size of the provided buffer
 * \param buf buffer to be filled with listed attributes
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_listxattr(liz_t *instance, liz_context_t *ctx, liz_inode_t ino, size_t size,
	          size_t *out_size, char *buf);

/*! \brief Remove extended attribute from a file
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param ino inode of a file
 * \param name attribute name
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_removexattr(liz_t *instance, liz_context_t *ctx, liz_inode_t ino, const char *name);

#ifdef __cplusplus
} // extern "C"
#endif

#endif
