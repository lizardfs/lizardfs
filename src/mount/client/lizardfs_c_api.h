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
#else
#include <stdbool.h>
#endif

enum liz_sugid_clear_mode {
	LIZARDFS_SUGID_CLEAR_MODE_NEVER,
	LIZARDFS_SUGID_CLEAR_MODE_ALWAYS,
	LIZARDFS_SUGID_CLEAR_MODE_OSX,
	LIZARDFS_SUGID_CLEAR_MODE_BSD,
	LIZARDFS_SUGID_CLEAR_MODE_EXT,
	LIZARDFS_SUGID_CLEAR_MODE_XFS,
	LIZARDFS_SUGID_CLEAR_MODE_END_
};

typedef struct liz_init_params {
	const char *bind_host;
	const char *host;
	const char *port;
	bool meta;
	const char *mountpoint;
	const char *subfolder;
	const char *password;
	const char *md5_pass;
	bool do_not_remember_password;
	bool delayed_init;
	unsigned report_reserved_period;

	unsigned io_retries;
	unsigned chunkserver_round_time_ms;
	unsigned chunkserver_connect_timeout_ms;
	unsigned chunkserver_wave_read_timeout_ms;
	unsigned total_read_timeout_ms;
	unsigned cache_expiration_time_ms;
	unsigned readahead_max_window_size_kB;
	bool prefetch_xor_stripes;
	double bandwidth_overuse;

	unsigned write_cache_size;
	unsigned write_workers;
	unsigned write_window_size;
	unsigned chunkserver_write_timeout_ms;
	unsigned cache_per_inode_percentage;
	unsigned symlink_cache_timeout_s;

	bool debug_mode;
	int keep_cache;
	double direntry_cache_timeout;
	unsigned direntry_cache_size;
	double entry_cache_timeout;
	double attr_cache_timeout;
	bool mkdir_copy_sgid;
	enum liz_sugid_clear_mode sugid_clear_mode;
	bool use_rw_lock;
	double acl_cache_timeout;
	unsigned acl_cache_size;

	bool verbose;

	const char *io_limits_config_file;
} liz_init_params_t;

#define LIZARDFS_MAX_GOAL_NAME 64
#define LIZARDFS_MAX_READLINK_LENGTH 65535

typedef uint32_t liz_inode_t;
typedef int liz_err_t;
struct liz;
typedef struct liz liz_t;
struct liz_fileinfo;
typedef struct liz_fileinfo liz_fileinfo_t;
struct liz_context;
typedef struct liz_context liz_context_t;
typedef struct liz_acl liz_acl_t;

#define LIZ_SET_ATTR_MODE      (1 << 0)
#define LIZ_SET_ATTR_UID       (1 << 1)
#define LIZ_SET_ATTR_GID       (1 << 2)
#define LIZ_SET_ATTR_SIZE      (1 << 3)
#define LIZ_SET_ATTR_ATIME     (1 << 4)
#define LIZ_SET_ATTR_MTIME     (1 << 5)
#define LIZ_SET_ATTR_ATIME_NOW (1 << 7)
#define LIZ_SET_ATTR_MTIME_NOW (1 << 8)

/* ACL flags */
#define LIZ_ACL_AUTO_INHERIT 0x01
#define LIZ_ACL_PROTECTED 0x02
#define LIZ_ACL_DEFAULTED 0x04
#define LIZ_ACL_WRITE_THROUGH 0x40
#define LIZ_ACL_MASKED 0x80

/* ACL ace types */
#define LIZ_ACL_ACCESS_ALLOWED_ACE_TYPE 0x0000
#define LIZ_ACL_ACCESS_DENIED_ACE_TYPE 0x0001

/* ACL ace flags bits */
#define LIZ_ACL_FILE_INHERIT_ACE 0x0001
#define LIZ_ACL_DIRECTORY_INHERIT_ACE 0x0002
#define LIZ_ACL_NO_PROPAGATE_INHERIT_ACE 0x0004
#define LIZ_ACL_INHERIT_ONLY_ACE 0x0008
#define LIZ_ACL_SUCCESSFUL_ACCESS_ACE_FLAG 0x00000010
#define LIZ_ACL_FAILED_ACCESS_ACE_FLAG 0x00000020
#define LIZ_ACL_IDENTIFIER_GROUP 0x0040
#define LIZ_ACL_INHERITED_ACE 0x0080  /* non nfs4 */
#define LIZ_ACL_SPECIAL_WHO 0x0100    /* lizardfs */

/* ACL ace mask bits */
#define LIZ_ACL_READ_DATA 0x00000001
#define LIZ_ACL_LIST_DIRECTORY 0x00000001
#define LIZ_ACL_WRITE_DATA 0x00000002
#define LIZ_ACL_ADD_FILE 0x00000002
#define LIZ_ACL_APPEND_DATA 0x00000004
#define LIZ_ACL_ADD_SUBDIRECTORY 0x00000004
#define LIZ_ACL_READ_NAMED_ATTRS 0x00000008
#define LIZ_ACL_WRITE_NAMED_ATTRS 0x00000010
#define LIZ_ACL_EXECUTE 0x00000020
#define LIZ_ACL_DELETE_CHILD 0x00000040
#define LIZ_ACL_READ_ATTRIBUTES 0x00000080
#define LIZ_ACL_WRITE_ATTRIBUTES 0x00000100
#define LIZ_ACL_WRITE_RETENTION 0x00000200
#define LIZ_ACL_WRITE_RETENTION_HOLD 0x00000400
#define LIZ_ACL_DELETE 0x00010000
#define LIZ_ACL_READ_ACL 0x00020000
#define LIZ_ACL_WRITE_ACL 0x00040000
#define LIZ_ACL_WRITE_OWNER 0x00080000
#define LIZ_ACL_SYNCHRONIZE 0x00100000

/* ACL ace special ids */
#define LIZ_ACL_OWNER_SPECIAL_ID 0x0
#define LIZ_ACL_GROUP_SPECIAL_ID 0x1
#define LIZ_ACL_EVERYONE_SPECIAL_ID 0x2

/* ACL helper macros */
#define LIZ_ACL_POSIX_MODE_READ (LIZ_ACL_READ_DATA | LIZ_ACL_LIST_DIRECTORY)
#define LIZ_ACL_POSIX_MODE_WRITE (LIZ_ACL_WRITE_DATA | LIZ_ACL_ADD_FILE \
	| LIZ_ACL_APPEND_DATA | LIZ_ACL_ADD_SUBDIRECTORY | LIZ_ACL_DELETE_CHILD)
#define LIZ_ACL_POSIX_MODE_EXECUTE (EXECUTE)
#define LIZ_ACL_POSIX_MODE_ALL (LIZ_ACL_POSIX_MODE_READ | LIZ_ACL_POSIX_MODE_WRITE \
	| LIZ_POSIX_MODE_EXEC)

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

/* Server location for a chunk part */
typedef struct liz_chunk_part_info {
	uint32_t addr;
	uint16_t port;
	uint16_t part_type_id;
	char *label;
} liz_chunk_part_info_t;

/* Chunk information including id, type and all parts */
typedef struct liz_chunk_info {
	uint64_t chunk_id;
	uint32_t chunk_version;
	uint32_t parts_size;
	liz_chunk_part_info_t *parts;
} liz_chunk_info_t;

typedef struct liz_chunkserver_info {
	uint32_t version;
	uint32_t ip;
	uint16_t port;
	uint64_t used_space;
	uint64_t total_space;
	uint32_t chunks_count;
	uint32_t error_counter;
	char *label;
} liz_chunkserver_info_t;

typedef struct liz_acl_ace {
	uint16_t type;
	uint16_t flags;
	uint32_t mask;
	uint32_t id;
} liz_acl_ace_t;

typedef struct liz_lock_info {
	int16_t l_type;   // Type of lock: F_RDLCK, F_WRLCK, or F_UNLCK.
	int64_t l_start;  // Offset where the lock begins.
	int64_t l_len;    // Size of the locked area; zero means until EOF.
	int32_t l_pid;    // Process holding the lock.
} liz_lock_info_t;

typedef struct liz_lock_interrupt_info {
	uint64_t owner;
	uint32_t ino;
	uint32_t reqid;
} liz_lock_interrupt_info_t;

/*!
 * \brief Function that registers lock interrupt data.
 * \param info lock info to be remembered somewhere
 * \param priv private data potentially needed by caller
 */
typedef int (*liz_lock_register_interrupt_t)(struct liz_lock_interrupt_info *info, void *priv);

/*!
 * \brief Function that registers lock interrupt data.
 */

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
 * \brief Set lock owner inside a fileinfo structure
 * \param fileinfo descriptor to an open file
 * \param lock_owner lock owner token
 */
void liz_set_lock_owner(liz_fileinfo_t *fileinfo, uint64_t lock_owner);

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
 * \brief Initialize init params to defaults
 * \param host master server connection host
 * \param port master server connection port
 * \param mountpoint a human-readable name for 'mountpoint' created by a connection
 */
void liz_set_default_init_params(struct liz_init_params *params,
		const char *host, const char *port, const char *mountpoint);

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
 * \brief Initialize a connection with master server
 * \param params init params initialized via liz_set_default_init_params()
 *        and possibly tweaked
 * \return a LizardFS client instance, nullptr if connection is impossible,
 *  sets last error code (check with liz_last_err())
 */
liz_t *liz_init_with_params(struct liz_init_params *params);

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
 * \param mode file permissions and node type
 * \param rdev major/minor numbers for block devices, otherwise ignored
 * \param entry filled upon successful creation
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_mknod(liz_t *instance, liz_context_t *ctx, liz_inode_t parent, const char *path,
	      mode_t mode, dev_t rdev, struct liz_entry *entry);

/*! \brief Create a link with given parent and name
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param inode target inode
 * \param parent parent inode
 * \param name link name (no paths allowed)
 * \param entry filled upon successful creation
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_link(liz_t *instance, liz_context_t *ctx, liz_inode_t inode, liz_inode_t parent,
	      const char *name, struct liz_entry *entry);

/*! \brief Create a symlink with given parent and name
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param link link contents (path it points to)
 * \param parent parent inode
 * \param name link name (no paths allowed)
 * \param entry filled upon successful creation
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_symlink(liz_t *instance, liz_context_t *ctx, const char *link, liz_inode_t parent,
	      const char *name, struct liz_entry *entry);

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
 * \param fileinfo descriptor of an open file
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_release(liz_t *instance, liz_fileinfo_t *fileinfo);

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
 * \param fileinfo descriptor of an open directory
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_releasedir(liz_t *instance, struct liz_fileinfo *fileinfo);

/*! \brief Read link contents
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param inode link inode
 * \param buf filled with result on success (no trailing '\0'),
 *        should be at least LIZARDFS_MAX_READLINK_LENGTH characters long
 * \param size allocated buf size
 * \return true link size on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_readlink(liz_t *instance, liz_context_t *ctx, liz_inode_t inode, char *buf, size_t size);

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
int liz_setxattr(liz_t *instance, liz_context_t *ctx, liz_inode_t ino,
                 const char *name, const uint8_t *value, size_t size,
                 enum liz_setxattr_mode mode);

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

/*!
 * \brief Create acl
 * \return acl entry
 * \post free memory with liz_destroy_acl call
 */
liz_acl_t *liz_create_acl();

/*!
 * \brief Create acl
 * \param mode  mode used to create the acl and set POSIX permission flags
 * \return acl entry
 * \post free memory with liz_destroy_acl call
 */
liz_acl_t *liz_create_acl_from_mode(unsigned int mode);

/*!
 * \brief Destroy acl
 * \param acl access control list
 */
void liz_destroy_acl(liz_acl_t *acl);

/*!
 * \brief Print acl in human readable format
 * \param acl access control list
 * \param buf buffer to be filled with acl representation
 * \param size buffer size
 * \param reply_size size needed to store acl representation
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_print_acl(liz_acl_t *acl, char *buf, size_t size, size_t *reply_size);

/*!
 * \brief Add access control entry to acl
 * \param acl access control list
 * \param ace prepared acl entry
 */
void liz_add_acl_entry(liz_acl_t *acl, const liz_acl_ace_t *ace);

/*!
 * \brief Get nth acl entry
 * \param acl access control list
 * \param n entry index
 * \param ace entry to be filled with data
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_get_acl_entry(const liz_acl_t *acl, int n, liz_acl_ace_t *ace);

/*!
 * \brief Get number of acl entries
 * \param acl access control list
 * \return number of entries
 */
size_t liz_get_acl_size(const liz_acl_t *acl);

/*!
 * \brief Set acl for a file
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param ino target inode
 * \param type acl type (access, default)
 * \param acl acl to be set
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_setacl(liz_t *instance, liz_context_t *ctx, liz_inode_t ino,
               liz_acl_t *acl);

/*!
 * \brief Get acl from a file
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param ino target inode
 * \param type acl type (access, default)
 * \param acl acl pointer to be filled with acl
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_getacl(liz_t *instance, liz_context_t *ctx, liz_inode_t ino,
	       liz_acl_t **acl);

/*!
 * \brief Apply rich acl masks to aces
 * \param acl acl to be modified
 * \param owner owner id
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_acl_apply_masks(liz_acl_t *acl, uint32_t owner);

/*! \brief Gather chunks information for a file
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param inode inode of a file
 * \param chunk_index index of first chunk to return
 * \param buffer preallocated buffer for chunk info
 * \param buffer_size size of preallocated buffer in number of elements
 * \param reply_size number of liz_chunk_info_t structures returned from master
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 * \post retrieved chunks information should be freed with liz_destroy_chunks_info call
 */
int liz_get_chunks_info(liz_t *instance, liz_context_t *ctx, liz_inode_t inode,
	                    uint32_t chunk_index, liz_chunk_info_t *buffer, uint32_t buffer_size,
	                    uint32_t *reply_size);

/*! \brief Free data allocated in liz_get_chunks_info
 * \param buffer buffer used in a successful liz_get_chunks_info call
 */
void liz_destroy_chunks_info(liz_chunk_info_t *buffer);

/*! \brief Gather information on chunkservers present in the cluster
 * \param instance instance returned from liz_init
 * \param servers buffer to be filled with server info
 * \param size buffer size in the number of elements
 * \param reply_size number of server entries returned from master
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 * \post retrieved chunkservers information should be freed with liz_destroy_chunkservers_info call
 */
int liz_get_chunkservers_info(liz_t *instance, liz_chunkserver_info_t *servers, uint32_t size,
	                 uint32_t *reply_size);

/*! \brief Free data allocated in liz_get_chunkservers_info
 * \param buffer buffer used in a successful liz_get_chunkservers_info call
 */
void liz_destroy_chunkservers_info(liz_chunkserver_info_t *buffer);

/*! \brief Put a lock on a file (semantics based on POSIX setlk)
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param fileinfo descriptor of an open file
 * \param lock lock information
 * \param handler function used to register lock interrupt data, can be NULL
 * \param priv private user data passed to handler
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 * \post interrupt data registered by handler can be later passed to liz_setlk_interrupt
 *       in order to cancel a lock request
 */
int liz_setlk(liz_t *instance, liz_context_t *ctx, liz_fileinfo_t *fileinfo,
	      const liz_lock_info_t *lock, liz_lock_register_interrupt_t handler, void *priv);

/*! \brief Get lock information from a file (semantics based on POSIX getlk)
 * \param instance instance returned from liz_init
 * \param ctx context returned from liz_create_context
 * \param fileinfo descriptor of an open file
 * \param lock lock buffer to be filled with lock information
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_getlk(liz_t *instance, liz_context_t *ctx, liz_fileinfo_t *fileinfo, liz_lock_info_t *lock);

/*! \brief Cancel a lock request
 * \param instance instance returned from liz_init
 * \param interrupt_info interrupt data saved by a function passed to setlk
 * \return 0 on success, -1 if failed, sets last error code (check with liz_last_err())
 */
int liz_setlk_interrupt(liz_t *instance, const liz_lock_interrupt_info_t *interrupt_info);
#ifdef __cplusplus
} // extern "C"
#endif

#endif
