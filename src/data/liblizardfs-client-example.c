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
#include <lizardfs/lizardfs_c_api.h>
#include <lizardfs/lizardfs_error_codes.h>

int main() {
	int err;
	liz_err_t liz_err = LIZARDFS_STATUS_OK;
	int i, r;
	liz_t *liz;
	liz_context_t *ctx;
	liz_chunkserver_info_t servers[65536];
	struct liz_fileinfo *fi;
	struct liz_entry entry, entry2;
	char buf[1024] = {0};

	/* Create a connection */
	ctx = liz_create_context();
	liz = liz_init("localhost", "9421", "test123");
	if (!liz) {
		fprintf(stderr, "Connection failed\n");
		liz_err = liz_last_err();
		goto destroy_context;
	}
	/* Try to create a file */
	err = liz_mknod(liz, ctx, LIZARDFS_INODE_ROOT, "testfile", 0755, 0, &entry);
	if (err) {
		fprintf(stderr, "File exists\n");
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

release_fileinfo:
	liz_release(liz, fi);
destroy_connection:
	liz_destroy(liz);
destroy_context:
	liz_destroy_context(ctx);

	printf("Program status: %s\n", liz_error_string(liz_err));
	return liz_error_conv(liz_err);
}
