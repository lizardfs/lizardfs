/*
 * Copyright 2017 Skytechnology sp. z o.o..
 * Author: Piotr Sarna <sarna@skytechnology.pl>
 *
 * LizardFS C API Example
 *
 * Compile with -llizardfs-client and LizardFS C/C++ library installed.
 */

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <lizardfs/lizardfs_c_api.h>
#include <lizardfs/lizardfs_error_codes.h>

/* Function that copies lizardfs lock interrupt data to provided buffer */
int register_interrupt(liz_lock_interrupt_info_t *info, void *priv) {
	memcpy(priv, info, sizeof(*info));
	return 0;
}

int main(int argc, char **argv) {
	int err;
	liz_err_t liz_err = LIZARDFS_STATUS_OK;
	int i, r;
	liz_t *liz;
	liz_context_t *ctx;
	liz_init_params_t params;
	liz_chunkserver_info_t servers[65536];
	struct liz_lock_info lock_info;
	struct liz_fileinfo *fi;
	struct liz_entry entry, entry2;
	struct liz_lock_interrupt_info lock_interrupt_info;
	char buf[1024] = {0};

	/* Create a connection */
	ctx = liz_create_context();
	liz_set_default_init_params(&params, "localhost", (argc > 1) ? argv[1] : "9421", "test123");
	liz = liz_init_with_params(&params);
	if (!liz) {
		fprintf(stderr, "Connection failed\n");
		liz_err = liz_last_err();
		goto destroy_context;
	}
	/* Try to unlink file if it exists and recreate it */
	liz_unlink(liz, ctx, LIZARDFS_INODE_ROOT, "testfile");
	err = liz_mknod(liz, ctx, LIZARDFS_INODE_ROOT, "testfile", 0755, 0, &entry);
	if (err) {
		liz_err = liz_last_err();
		goto destroy_context;
	}
	/* Check if newly created file can be looked up */
	err = liz_lookup(liz, ctx, LIZARDFS_INODE_ROOT, "testfile", &entry2);
	assert(entry.ino == entry2.ino);
	/* Open a file */
	fi = liz_open(liz, ctx, entry.ino, O_RDWR);
	if (!fi) {
		fprintf(stderr, "Open failed\n");
		liz_err = liz_last_err();
		goto destroy_connection;
	}
	/* Write to a file */
	r = liz_write(liz, ctx, fi, 0, 8, "abcdefghijkl");
	if (r < 0) {
		fprintf(stderr, "Write failed\n");
		liz_err = liz_last_err();
		goto release_fileinfo;
	}
	/* Read from a file */
	r = liz_read(liz, ctx, fi, 4, 3, buf);
	if (r < 0) {
		fprintf(stderr, "Read failed\n");
		liz_err = liz_last_err();
		goto release_fileinfo;
	}
	printf("Read %.3s from inode %u\n", buf, entry.ino);

	/* Get chunkservers info */
	uint32_t reply_size;
	r = liz_get_chunkservers_info(liz, servers, 65536, &reply_size);
	if (r < 0) {
		fprintf(stderr, "Chunkserver info failed\n");
		liz_err = liz_last_err();
		goto release_fileinfo;
	}
	for (i = 0; i < reply_size; ++i) {
		printf("* Chunkserver %u:%u with label %s\n",
	               servers[i].ip, servers[i].port, servers[i].label);
	}
	liz_destroy_chunkservers_info(servers);

	/* Set and get access control lists */
	liz_acl_t *acl = liz_create_acl();
	liz_acl_ace_t acl_ace = {LIZ_ACL_ACCESS_ALLOWED_ACE_TYPE, LIZ_ACL_SPECIAL_WHO, LIZ_ACL_POSIX_MODE_WRITE, 0};
	liz_add_acl_entry(acl, &acl_ace);
	acl_ace.id = 100;
	acl_ace.flags &= ~LIZ_ACL_SPECIAL_WHO;
	acl_ace.mask |= LIZ_ACL_POSIX_MODE_WRITE;
	liz_add_acl_entry(acl, &acl_ace);
	acl_ace.id = 101;
	acl_ace.flags |= LIZ_ACL_IDENTIFIER_GROUP;
	acl_ace.mask &= ~LIZ_ACL_APPEND_DATA;
	acl_ace.mask |= LIZ_ACL_WRITE_ACL;
	liz_add_acl_entry(acl, &acl_ace);
	size_t acl_reply_size;
	char acl_buf[256] = {};
	r = liz_print_acl(acl, acl_buf, 256, &acl_reply_size);
	if (r < 0) {
		fprintf(stderr, "Printing acl failed\n");
		liz_err = liz_last_err();
		goto release_fileinfo;
	}
	printf("[%d %lu] ACL to set: %s\n", r, acl_reply_size, acl_buf);
	r = liz_setacl(liz, ctx, entry.ino, acl);
	if (r < 0) {
		fprintf(stderr, "setacl failed\n");
		liz_err = liz_last_err();
		goto release_fileinfo;
	}
	liz_destroy_acl(acl);

	memset(acl_buf, 0, 256);
	r = liz_getacl(liz, ctx, entry.ino, &acl);
	if (r < 0) {
		fprintf(stderr, "Getting acl failed\n");
		liz_err = liz_last_err();
		goto release_fileinfo;
	}
	size_t acl_size = liz_get_acl_size(acl);
	printf("ACL size=%lu\n", acl_size);
	for (i = 0; i < acl_size; ++i) {
		liz_get_acl_entry(acl, i, &acl_ace);
		printf("entry %u %u %x\n", acl_ace.id, acl_ace.type, acl_ace.mask);
	}
	r = liz_print_acl(acl, acl_buf, 256, &acl_reply_size);
	if (r < 0) {
		fprintf(stderr, "Printing acl failed\n");
		liz_err = liz_last_err();
		goto release_fileinfo;
	}
	printf("[%d %lu] ACL extracted: %s\n", r, acl_reply_size, acl_buf);
	liz_destroy_acl(acl);

	lock_info.l_type = F_WRLCK;
	lock_info.l_start = 0;
	lock_info.l_len = 3;
	lock_info.l_pid = 19;

	r = liz_setlk(liz, ctx, fi, &lock_info, NULL, NULL);
	if (r < 0) {
		fprintf(stderr, "Setlk failed\n");
		liz_err = liz_last_err();
		goto release_fileinfo;
	}
	printf("Lock info 1: %d %ld %ld %d\n", lock_info.l_type, lock_info.l_start, lock_info.l_len, lock_info.l_pid);

	r = liz_getlk(liz, ctx, fi, &lock_info);
	if (r < 0) {
		fprintf(stderr, "Getlk failed\n");
		liz_err = liz_last_err();
		goto release_fileinfo;
	}
	printf("Lock info 2: %d %ld %ld %d\n", lock_info.l_type, lock_info.l_start, lock_info.l_len, lock_info.l_pid);

	lock_info.l_type = F_UNLCK;
	lock_info.l_len = 1;
	r = liz_setlk(liz, ctx, fi, &lock_info, NULL, NULL);
	if (r < 0) {
		fprintf(stderr, "Setlk failed\n");
		liz_err = liz_last_err();
		goto release_fileinfo;
	}
	lock_info.l_type = F_WRLCK;
	lock_info.l_len = 2;
	r = liz_getlk(liz, ctx, fi, &lock_info);
	if (r < 0) {
		fprintf(stderr, "Getlk2 failed\n");
		liz_err = liz_last_err();
		goto release_fileinfo;
	}
	printf("Lock info 3: %d %ld %ld %d\n", lock_info.l_type, lock_info.l_start, lock_info.l_len, lock_info.l_pid);

	lock_info.l_type = F_WRLCK;
	lock_info.l_len = 3;
	r = liz_setlk(liz, ctx, fi, &lock_info, &register_interrupt, &lock_interrupt_info);
	if (r < 0) {
		fprintf(stderr, "Setlk failed\n");
		liz_err = liz_last_err();
		goto release_fileinfo;
	}
	printf("Filled interrupt info: %lx %u %u\n", lock_interrupt_info.owner,
	       lock_interrupt_info.ino, lock_interrupt_info.reqid);

release_fileinfo:
	liz_release(liz, fi);
destroy_connection:
	liz_destroy(liz);
destroy_context:
	liz_destroy_context(ctx);

	printf("Program status: %s\n", liz_error_string(liz_err));
	return liz_error_conv(liz_err);
}
