/*
   Copyright 2008 Gemius SA.

   This file is part of MooseFS.

   MooseFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   MooseFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with MooseFS.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <fuse_lowlevel.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <syslog.h>

#include "config.h"
#include "mfs_fuse.h"
#include "mfs_meta_fuse.h"

#include "mastercomm.h"
#include "readdata.h"
#include "writedata.h"

#define METAOPT	"default_permissions,fsname=MFSMETA"
#ifdef NODEFPERM
#  define OPTDEFPERM ""
#else
#  define OPTDEFPERM ",default_permissions"
#endif
#ifdef __sun
#  define OPTDEV ""
#else
#  define OPTDEV ",dev"
#endif
#define MAINOPT "allow_other" OPTDEV ",suid" OPTDEFPERM ",fsname=MFS"

static struct fuse_lowlevel_ops mfs_meta_oper = {
	.statfs		= mfs_meta_statfs,
	.lookup		= mfs_meta_lookup,
	.getattr	= mfs_meta_getattr,
	.setattr	= mfs_meta_setattr,
	.unlink		= mfs_meta_unlink,
	.rename		= mfs_meta_rename,
	.opendir	= mfs_meta_opendir,
	.readdir	= mfs_meta_readdir,
	.releasedir	= mfs_meta_releasedir,
	.open		= mfs_meta_open,
	.release	= mfs_meta_release,
	.read		= mfs_meta_read,
	.write		= mfs_meta_write,
//	.access		= mfs_meta_access
};

static struct fuse_lowlevel_ops mfs_oper = {
	.statfs		= mfs_statfs,
	.lookup		= mfs_lookup,
	.getattr	= mfs_getattr,
	.setattr	= mfs_setattr,
	.mknod		= mfs_mknod,
	.unlink		= mfs_unlink,
	.mkdir		= mfs_mkdir,
	.rmdir		= mfs_rmdir,
	.symlink	= mfs_symlink,
	.readlink	= mfs_readlink,
	.rename		= mfs_rename,
	.link		= mfs_link,
	.opendir	= mfs_opendir,
	.readdir	= mfs_readdir,
	.releasedir	= mfs_releasedir,
	.create		= mfs_create,
	.open		= mfs_open,
	.release	= mfs_release,
	.flush		= mfs_flush,
	.fsync		= mfs_fsync,
	.read		= mfs_read,
	.write		= mfs_write,
	.access		= mfs_access,
};

void usage(const char *name) {
	fprintf(stderr,"mfsmount v. %d.%d.%d\n",VERSMAJ,VERSMID,VERSMIN);
	fprintf(stderr,"usage: %s [-r][-m][-d] [-h master host] [-p master port] [-l path] [-w mount point]\n",name);
	fprintf(stderr,"\nr: readonly mode\nm: mount metadata\nd: fuse debug mode\n");
	fprintf(stderr,"\ndefaults:\n\th: mfsmaster\n\tp: 9421\n\tl: /\n\tw: /mnt/mfs\n");
}

int main(int argc, char *argv[]) {
	struct fuse_args args;
	struct fuse_session *se;
	struct fuse_chan *ch;
	struct rlimit rls;
//	char *mountpoint;
	int debug_mode;
	int local_mode;
	int meta_mode;
	int ro;
	char c,*host,*port,*path,*mp;
	int err = -1;
	int fd;
//	int mt,fg;

	debug_mode = 0;
	local_mode = 0;
	meta_mode = 0;
	ro = 0;
	host = port = path = mp = NULL;
	while ((c=getopt(argc,argv,"rmcv:h:p:l:w:?"))!=-1) {
		switch (c) {
		case 'r':
			ro=1;
			break;
		case 'm':
			meta_mode=1;
			break;
		case 'v':
			debug_mode=strtol(optarg,NULL,10);
			break;
		case 'c':
			local_mode=1;
			break;
		case 'h':
			host = strdup(optarg);
			break;
		case 'p':
			port = strdup(optarg);
			break;
		case 'l':
			path = strdup(optarg);
			break;
		case 'w':
			mp = strdup(optarg);
			break;
		default:
			fprintf(stderr,"unknown parameter\n");
		case '?':
			usage(argv[0]);
			return 1;
		}
	}
	if (host==NULL) {
		host = strdup("mfsmaster");
	}
	if (port==NULL) {
		port = strdup("9421");
	}
	if (path==NULL) {
		path = strdup("/");
	}
	if (mp==NULL) {
		if (meta_mode) {
			mp = strdup("/mnt/mfsmeta");
		} else {
			mp = strdup("/mnt/mfs");
		}
	}
	if (debug_mode>=2) {
		args.argc = 4;
	} else {
		args.argc = 3;
	}
	args.argv = (char**)malloc(sizeof(char*)*args.argc);
	args.argv[0] = strdup(argv[0]);
	args.argv[1] = strdup("-o");
	if (meta_mode) {
		if (ro) {
			args.argv[2] = strdup("ro," METAOPT);
		} else {
			args.argv[2] = strdup(METAOPT);
		}
	} else {
		if (ro) {
			args.argv[2] = strdup("ro," MAINOPT);
		} else {
			args.argv[2] = strdup(MAINOPT);
		}
	}
	if (debug_mode>=2) {
		args.argv[3] = strdup("-d");
	}
	args.allocated = 1;

//	args.argc = argc;
//	args.argv = argv;
//	args.allocated = 0;

 //	if (fuse_parse_cmdline(&args, &mountpoint, NULL, NULL)<0) {
//		fprintf(stderr,"error in fuse_parse_cmdline\n");
///		fuse_opt_free_args(&args);
//		return 1;
//	}
 	fd = fuse_mount(mp, &args);
	if (fd<0) {
		fprintf(stderr,"error in fuse_mount\n");
		fuse_opt_free_args(&args);
		return 1;
	}

	if (meta_mode) {
		mfs_meta_init(debug_mode,local_mode);
		se = fuse_lowlevel_new(&args, &mfs_meta_oper, sizeof(mfs_meta_oper), NULL);
	} else {
		mfs_init(debug_mode,local_mode);
		se = fuse_lowlevel_new(&args, &mfs_oper, sizeof(mfs_oper), NULL);
	}
	if (se==NULL) {
		fprintf(stderr,"error in fuse_lowlevel_new\n");
		close(fd);
		fuse_unmount(mp);
		fuse_opt_free_args(&args);
		return 1;
	}

	ch = fuse_kern_chan_new(fd);
	if (ch==NULL) {
		fprintf(stderr,"error in fuse_kern_chan_new\n");
		fuse_session_destroy(se);
		close(fd);
		fuse_unmount(mp);
		fuse_opt_free_args(&args);
		return 1;
	}
	fuse_session_add_chan(se, ch);

	err = fork();
	if (err<0) {
		fprintf(stderr,"fork error\n");
		fuse_session_destroy(se);
		close(fd);
		fuse_unmount(mp);
		fuse_opt_free_args(&args);
		return 1;
	} else if (err>0) {
		return 0;
	}

	rls.rlim_cur = 100000;
	rls.rlim_max = 100000;
	setrlimit(RLIMIT_NOFILE,&rls);

	if (fuse_set_signal_handlers(se)<0) {
		fprintf(stderr,"error in fuse_set_signal_handlers\n");
		fuse_session_destroy(se);
		close(fd);
		fuse_unmount(mp);
		fuse_opt_free_args(&args);
		return 1;
	}

	if (debug_mode==0) {
		int i;
		setsid();
		setpgid(0,getpid());
		if ((i = open("/dev/null", O_RDWR, 0)) != -1) {
			(void)dup2(i, STDIN_FILENO);
			(void)dup2(i, STDOUT_FILENO);
			(void)dup2(i, STDERR_FILENO);
			if (i>2) close (i);
		}
	}

	fs_init(host,port);

	if (mfs_rootnode_setup(path)<0) {
		syslog(LOG_ERR,"mfs path not found");
		fuse_remove_signal_handlers(se);
		fuse_session_destroy(se);
		close(fd);
		fuse_unmount(mp);
		fuse_opt_free_args(&args);
		return 1;
	}

	if (meta_mode==0) {
		read_data_init();
		write_data_init();
	}

	err = fuse_session_loop_mt(se);
	//err = fuse_session_loop(se);
	fuse_remove_signal_handlers(se);
	fuse_session_destroy(se);
	close(fd);
	fuse_unmount(mp);
	fuse_opt_free_args(&args);
	return err ? 1 : 0;
}
