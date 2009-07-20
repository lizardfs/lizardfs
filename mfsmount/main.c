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

#include "config.h"

#include <fuse_lowlevel.h>
#include <fuse_opt.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <unistd.h>
#include <pwd.h>
#include <grp.h>
#include <syslog.h>

#include "mfs_fuse.h"
#include "mfs_meta_fuse.h"

#include "MFSCommunication.h"
#include "md5.h"
#include "mastercomm.h"
#include "readdata.h"
#include "writedata.h"
#include "csdb.h"

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)
const char id1[]="@(#) version: " STR(VERSMAJ) "." STR(VERSMID) "." STR(VERSMIN) ", written by Jakub Kruszona-Zawadzki";
const char id2[]="@(#) Copyright by Gemius S.A.";

static void mfs_fsinit (void *userdata, struct fuse_conn_info *conn);

static struct fuse_lowlevel_ops mfs_meta_oper = {
	.init           = mfs_fsinit,
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
	.init           = mfs_fsinit,
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
#if FUSE_VERSION >= 26
/* locks are still in development
	.getlk		= mfs_getlk,
	.setlk		= mfs_setlk,
*/
#endif
};

struct mfsopts {
	char *masterhost;
	char *masterport;
	char *subfolder;
	char *password;
	char *md5pass;
	unsigned nofile;
	int nostdmountoptions;
	int meta;
	int debug;
	int cachefiles;
	double attrcacheto;
	double entrycacheto;
	double direntrycacheto;
};

static struct mfsopts mfsopts;

enum {
	KEY_META,
	KEY_HOST,
	KEY_PORT,
	KEY_PATH,
	KEY_PASSWORDASK,
	KEY_NOSTDMOUNTOPTIONS,
	KEY_HELP,
	KEY_VERSION,
};

#define MFS_OPT(t, p, v) { t, offsetof(struct mfsopts, p), v }

static struct fuse_opt mfs_opts[] = {
	MFS_OPT("mfsmaster=%s", masterhost, 0),
	MFS_OPT("mfsport=%s", masterport, 0),
	MFS_OPT("mfssubfolder=%s", subfolder, 0),
	MFS_OPT("mfspassword=%s", password, 0),
	MFS_OPT("mfsmd5pass=%s", md5pass, 0),
	MFS_OPT("mfsrlimitnofile=%u", nofile, 0),
	MFS_OPT("mfsdebug", debug, 1),
	MFS_OPT("mfsmeta", meta, 1),
	MFS_OPT("mfscachefiles", cachefiles, 1),
	MFS_OPT("mfsattrcacheto=%lf", attrcacheto, 0),
	MFS_OPT("mfsentrycacheto=%lf", entrycacheto, 0),
	MFS_OPT("mfsdirentrycacheto=%lf", direntrycacheto, 0),

	FUSE_OPT_KEY("-m",             KEY_META),
	FUSE_OPT_KEY("--meta",         KEY_META),
	FUSE_OPT_KEY("-H ",            KEY_HOST),
	FUSE_OPT_KEY("-P ",            KEY_PORT),
	FUSE_OPT_KEY("-S ",            KEY_PATH),
	FUSE_OPT_KEY("-p",             KEY_PASSWORDASK),
	FUSE_OPT_KEY("--password",     KEY_PASSWORDASK),
	FUSE_OPT_KEY("-n",             KEY_NOSTDMOUNTOPTIONS),
	FUSE_OPT_KEY("--nostdopts",    KEY_NOSTDMOUNTOPTIONS),
	FUSE_OPT_KEY("-V",             KEY_VERSION),
	FUSE_OPT_KEY("--version",      KEY_VERSION),
	FUSE_OPT_KEY("-h",             KEY_HELP),
	FUSE_OPT_KEY("--help",         KEY_HELP),
	FUSE_OPT_END
};

static void usage(const char *progname) {
	fprintf(stderr,
"usage: %s mountpoint [options]\n"
"\n"
"general options:\n"
"    -o opt,[opt...]         mount options\n"
"    -h   --help             print help\n"
"    -V   --version          print version\n"
"\n"
"MFS options:\n"
"    -m   --meta                 equivalent to '-o mfsmeta'\n"
"    -H HOST                     equivalent to '-o mfsmaster=HOST'\n"
"    -P PORT                     equivalent to '-o mfsport=PORT'\n"
"    -S PATH                     equivalent to '-o mfssubfolder=PATH'\n"
"    -p   --password             similar to '-o mfspassword=PASSWORD', but show prompt and ask user for password\n"
"    -n   --nostdopts            do not add standard MFS mount options: '-o allow_other,default_permissions,fsname=MFS'\n"
"    -o mfsdebug                 print some debugging information\n"
"    -o mfsmeta                  mount meta filesystem (trash etc.)\n"
"    -o mfscachefiles            preserve files data in cache\n"
"    -o mfsattrcacheto=SEC       set attributes cache timeout in seconds (default: 1.0)\n"
"    -o mfsentrycacheto=SEC      set file entry cache timeout in seconds (default: 0.0)\n"
"    -o mfsdirentrycacheto=SEC   set directory entry cache timeout in seconds (default: 1.0)\n"
"    -o mfsrlimitnofile=N        on startup mfsmount tries to change number of descriptors it can simultaneously open (default: 100000)\n"
"    -o mfsmaster=HOST           define mfsmaster location (default: mfsmaster)\n"
"    -o mfsport=PORT             define mfsmaster port number (default: 9421)\n"
"    -o mfssubfolder=PATH        define subfolder to mount as root (default: /)\n"
"    -o mfspassword=PASSWORD     authenticate to mfsmaster with password\n"
"    -o mfsmd5pass=MD5           authenticate to mfsmaster using directly given md5 (only if mfspassword is not defined)\n"
"\n", progname);
}

// return value:
//   0 - discard this arg
//   1 - keep this arg for future processing
static int mfs_opt_proc(void *data, const char *arg, int key, struct fuse_args *outargs) {
	(void)data;

	switch (key) {
	case FUSE_OPT_KEY_OPT:
		return 1;
	case FUSE_OPT_KEY_NONOPT:
		return 1;
	case KEY_HOST:
		if (mfsopts.masterhost==NULL) {
			mfsopts.masterhost = strdup(arg+2);
		}
		return 0;
	case KEY_PORT:
		if (mfsopts.masterport==NULL) {
			mfsopts.masterport = strdup(arg+2);
		}
		return 0;
	case KEY_PATH:
		if (mfsopts.subfolder==NULL) {
			mfsopts.subfolder = strdup(arg+2);
		}
		return 0;
	case KEY_PASSWORDASK:
		if (mfsopts.password!=NULL) {
			free(mfsopts.password);
		}
		mfsopts.password = getpass("Password:");
		return 0;
	case KEY_META:
		mfsopts.meta = 1;
		return 0;
	case KEY_NOSTDMOUNTOPTIONS:
		mfsopts.nostdmountoptions = 1;
		return 0;
	case KEY_VERSION:
		fprintf(stderr, "MFS version %u.%u.%u\n",VERSMAJ,VERSMID,VERSMIN);
		fuse_opt_add_arg(outargs, "--version");
		fuse_parse_cmdline(outargs,NULL,NULL,NULL);
		exit(0);
	case KEY_HELP:
		usage(outargs->argv[0]);
		fuse_opt_add_arg(outargs, "-ho");
		fuse_parse_cmdline(outargs,NULL,NULL,NULL);
		exit(1);
	default:
		fprintf(stderr, "internal error\n");
		abort();
	}
}

static void mfs_fsinit (void *userdata, struct fuse_conn_info *conn) {
	int *piped = (int*)userdata;
	char s;
	(void)conn;
	if (piped[1]>=0) {
		s=0;
		if (write(piped[1],&s,1)!=1) {
			syslog(LOG_ERR,"pipe write error: %m");
		}
		close(piped[1]);
	}
}

int mainloop(struct fuse_args *args,const char* mp,int mt,int fg) {
	struct fuse_session *se;
	struct fuse_chan *ch;
	struct rlimit rls;
	int piped[2];
	char s;
	int err;
	uint8_t sesflags;
	uint32_t rootuid,rootgid;
	int i,j;
	const char *sesflagposstrtab[]={SESFLAG_POS_STRINGS};
	const char *sesflagnegstrtab[]={SESFLAG_NEG_STRINGS};
	struct passwd *pw;
	struct group *gr;
	md5ctx ctx;
	uint8_t md5pass[16];

	if (mfsopts.password) {
		md5_init(&ctx);
		md5_update(&ctx,(uint8_t*)(mfsopts.password),strlen(mfsopts.password));
		md5_final(md5pass,&ctx);
		memset(mfsopts.password,0,strlen(mfsopts.password));
	} else if (mfsopts.md5pass) {
		uint8_t *p = (uint8_t*)(mfsopts.md5pass);
		for (i=0 ; i<16 ; i++) {
			if (*p>='0' && *p<='9') {
				md5pass[i]=(*p-'0')<<4;
			} else if (*p>='a' && *p<='f') {
				md5pass[i]=(*p-'a'+10)<<4;
			} else if (*p>='A' && *p<='F') {
				md5pass[i]=(*p-'A'+10)<<4;
			} else {
				fprintf(stderr,"bad md5 definition (md5 should be given as 32 hex digits)\n");
				return 1;
			}
			p++;
			if (*p>='0' && *p<='9') {
				md5pass[i]+=(*p-'0');
			} else if (*p>='a' && *p<='f') {
				md5pass[i]+=(*p-'a'+10);
			} else if (*p>='A' && *p<='F') {
				md5pass[i]+=(*p-'A'+10);
			} else {
				fprintf(stderr,"bad md5 definition (md5 should be given as 32 hex digits)\n");
				return 1;
			}
			p++;
		}
		if (*p) {
			fprintf(stderr,"bad md5 definition (md5 should be given as 32 hex digits)\n");
			return 1;
		}
		memset(mfsopts.md5pass,0,strlen(mfsopts.md5pass));
	}


	if (fs_init_master_connection(mfsopts.masterhost,mfsopts.masterport,mfsopts.meta,mp,mfsopts.subfolder,(mfsopts.password||mfsopts.md5pass)?md5pass:NULL,&sesflags,&rootuid,&rootgid)<0) {
		return 1;
	}
	memset(md5pass,0,16);

	if (mfsopts.debug) {
		fprintf(stderr,"registered to master\n");
	}

	fprintf(stderr,"mfsmaster accepted connection with parameters: ");
	j=0;
	for (i=0 ; i<8 ; i++) {
		if (sesflags&(1<<i)) {
			fprintf(stderr,"%s%s",j?",":"",sesflagposstrtab[i]);
			j=1;
		} else if (sesflagnegstrtab[i]) {
			fprintf(stderr,"%s%s",j?",":"",sesflagnegstrtab[i]);
			j=1;
		}
	}
	if (j==0) {
		fprintf(stderr,"-");
	}
	if (mfsopts.meta==0) {
		fprintf(stderr,"; root mapped to ");
		pw = getpwuid(rootuid);
		if (pw) {
			fprintf(stderr,"%s:",pw->pw_name);
		} else {
			fprintf(stderr,"%"PRIu32":",rootuid);
		}
		gr = getgrgid(rootgid);
		if (gr) {
			fprintf(stderr,"%s\n",gr->gr_name);
		} else {
			fprintf(stderr,"%"PRIu32"\n",rootgid);
		}
	} else {
		fprintf(stderr,"\n");
	}

	piped[0] = piped[1] = -1;
	if (fg==0) {
		if (pipe(piped)<0) {
			fprintf(stderr,"pipe error\n");
			return 1;
		}
		err = fork();
		if (err<0) {
			fprintf(stderr,"fork error\n");
			return 1;
		} else if (err>0) {
			close(piped[1]);
			err = read(piped[0],&s,1);
			if (err==0) {
				s=1;
			}
			return s;
		}
		close(piped[0]);
		s=1;
	}

	fs_init_threads();

	if (mfsopts.meta==0) {
		read_data_init();
		write_data_init();
		csdb_init();
	}

 	ch = fuse_mount(mp, args);
	if (ch==NULL) {
		fprintf(stderr,"error in fuse_mount\n");
		if (piped[1]>=0) {
			if (write(piped[1],&s,1)!=1) {
				fprintf(stderr,"pipe write error\n");
			}
			close(piped[1]);
		}
		return 1;
	}

	if (mfsopts.meta) {
		mfs_meta_init(mfsopts.debug,mfsopts.entrycacheto,mfsopts.attrcacheto);
		se = fuse_lowlevel_new(args, &mfs_meta_oper, sizeof(mfs_meta_oper), (void*)piped);
	} else {
		mfs_init(mfsopts.debug,mfsopts.cachefiles,mfsopts.direntrycacheto,mfsopts.entrycacheto,mfsopts.attrcacheto);
		se = fuse_lowlevel_new(args, &mfs_oper, sizeof(mfs_oper), (void*)piped);
	}
	if (se==NULL) {
		fuse_unmount(mp,ch);
		fprintf(stderr,"error in fuse_lowlevel_new\n");
		usleep(100000);	// time for print other error messages by FUSE
		if (piped[1]>=0) {
			if (write(piped[1],&s,1)!=1) {
				fprintf(stderr,"pipe write error\n");
			}
			close(piped[1]);
		}
		return 1;
	}

//	fprintf(stderr,"check\n");
	fuse_session_add_chan(se, ch);

	rls.rlim_cur = mfsopts.nofile;
	rls.rlim_max = mfsopts.nofile;
	setrlimit(RLIMIT_NOFILE,&rls);

	if (fuse_set_signal_handlers(se)<0) {
		fprintf(stderr,"error in fuse_set_signal_handlers\n");
		fuse_session_remove_chan(ch);
		fuse_session_destroy(se);
		fuse_unmount(mp,ch);
		if (piped[1]>=0) {
			if (write(piped[1],&s,1)!=1) {
				fprintf(stderr,"pipe write error\n");
			}
			close(piped[1]);
		}
		return 1;
	}

	if (mfsopts.debug==0 && fg==0) {
		setsid();
		setpgid(0,getpid());
		if ((i = open("/dev/null", O_RDWR, 0)) != -1) {
			(void)dup2(i, STDIN_FILENO);
			(void)dup2(i, STDOUT_FILENO);
			(void)dup2(i, STDERR_FILENO);
			if (i>2) close (i);
		}
	}

	if (mt) {
		err = fuse_session_loop_mt(se);
	} else {
		err = fuse_session_loop(se);
	}
	if (err) {
		if (piped[1]>=0) {
			if (write(piped[1],&s,1)!=1) {
				syslog(LOG_ERR,"pipe write error: %m");
			}
			close(piped[1]);
		}
	}
	fuse_remove_signal_handlers(se);
	fuse_session_remove_chan(ch);
	fuse_session_destroy(se);
	fuse_unmount(mp,ch);
	return err ? 1 : 0;
}

#if FUSE_VERSION == 25
static int fuse_opt_insert_arg(struct fuse_args *args, int pos, const char *arg) {
	assert(pos <= args->argc);
	if (fuse_opt_add_arg(args, arg) == -1) {
		return -1;
	}
	if (pos != args->argc - 1) {
		char *newarg = args->argv[args->argc - 1];
		memmove(&args->argv[pos + 1], &args->argv[pos], sizeof(char *) * (args->argc - pos - 1));
		args->argv[pos] = newarg;
	}
	return 0;
}
#endif

static unsigned int strncpy_remove_commas(char *dstbuff, unsigned int dstsize,char *src) {
	char c;
	unsigned int l;
	l=0;
	while ((c=*src++) && l+1<dstsize) {
		if (c!=',') {
			*dstbuff++ = c;
			l++;
		}
	}
	*dstbuff=0;
	return l;
}

#if FUSE_VERSION >= 27
static unsigned int strncpy_escape_commas(char *dstbuff, unsigned int dstsize,char *src) {
	char c;
	unsigned int l;
	l=0;
	while ((c=*src++) && l+1<dstsize) {
		if (c!=',' && c!='\\') {
			*dstbuff++ = c;
			l++;
		} else {
			if (l+2<dstsize) {
				*dstbuff++ = '\\';
				*dstbuff++ = c;
				l+=2;
			} else {
				*dstbuff=0;
				return l;
			}
		}
	}
	*dstbuff=0;
	return l;
}
#endif

void make_fsname(struct fuse_args *args) {
	char fsnamearg[256];
	unsigned int l;
	int libver;
#if FUSE_VERSION >= 27
	libver = fuse_version();
//	assert(libver >= 27);
	l = snprintf(fsnamearg,256,"-osubtype=mfs%s,fsname=",(mfsopts.meta)?"meta":"");
	if (libver >= 28) {
		l += strncpy_escape_commas(fsnamearg+l,256-l,mfsopts.masterhost);
		if (l<255) {
			fsnamearg[l++]=':';
		}
		l += strncpy_escape_commas(fsnamearg+l,256-l,mfsopts.masterport);
		if (mfsopts.subfolder[0]!='/') {
			if (l<255) {
				fsnamearg[l++]='/';
			}
		}
		if (mfsopts.subfolder[0]!='/' && mfsopts.subfolder[1]!=0) {
			l += strncpy_escape_commas(fsnamearg+l,256-l,mfsopts.subfolder);
		}
		fsnamearg[255]=0;
	} else {
		l += strncpy_remove_commas(fsnamearg+l,256-l,mfsopts.masterhost);
		if (l<255) {
			fsnamearg[l++]=':';
		}
		l += strncpy_remove_commas(fsnamearg+l,256-l,mfsopts.masterport);
		if (mfsopts.subfolder[0]!='/') {
			if (l<255) {
				fsnamearg[l++]='/';
			}
		}
		if (mfsopts.subfolder[0]!='/' && mfsopts.subfolder[1]!=0) {
			l += strncpy_remove_commas(fsnamearg+l,256-l,mfsopts.subfolder);
		}
		fsnamearg[255]=0;
	}
#else
	l = snprintf(fsnamearg,256,"-ofsname=mfs%s#",(mfsopts.meta)?"meta":"");
	l += strncpy_remove_commas(fsnamearg+l,256-l,mfsopts.masterhost);
	if (l<255) {
		fsnamearg[l++]=':';
	}
	l += strncpy_remove_commas(fsnamearg+l,256-l,mfsopts.masterport);
	if (mfsopts.subfolder[0]!='/') {
		if (l<255) {
			fsnamearg[l++]='/';
		}
	}
	if (mfsopts.subfolder[0]!='/' && mfsopts.subfolder[1]!=0) {
		l += strncpy_remove_commas(fsnamearg+l,256-l,mfsopts.subfolder);
	}
	fsnamearg[255]=0;
#endif
	fuse_opt_insert_arg(args, 1, fsnamearg);
}

int main(int argc, char *argv[]) {
	int res;
	int mt,fg;
	char *mountpoint;
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

	mfsopts.masterhost = NULL;
	mfsopts.masterport = NULL;
	mfsopts.subfolder = NULL;
	mfsopts.password = NULL;
	mfsopts.md5pass = NULL;
	mfsopts.nofile = 0;
	mfsopts.nostdmountoptions = 0;
	mfsopts.meta = 0;
	mfsopts.debug = 0;
	mfsopts.cachefiles = 0;
	mfsopts.attrcacheto = 1.0;
	mfsopts.entrycacheto = 0.0;
	mfsopts.direntrycacheto = 1.0;

	if (fuse_opt_parse(&args, &mfsopts, mfs_opts, mfs_opt_proc)<0) {
		exit(1);
	}

	if (mfsopts.masterhost==NULL) {
		mfsopts.masterhost = strdup("mfsmaster");
	}
	if (mfsopts.masterport==NULL) {
		mfsopts.masterport = strdup("9421");
	}
	if (mfsopts.subfolder==NULL) {
		mfsopts.subfolder = strdup("/");
	}
	if (mfsopts.nofile==0) {
		mfsopts.nofile=100000;
	}

	if (mfsopts.nostdmountoptions==0) {
		fuse_opt_add_arg(&args, "-oallow_other,default_permissions");
	}


	make_fsname(&args);

	fuse_parse_cmdline(&args,&mountpoint,&mt,&fg);

	if (!mountpoint) {
		fprintf(stderr,"no mount point\nsee: %s -h for help\n",argv[0]);
		return 1;
	}

	res = mainloop(&args,mountpoint,mt,fg);
	fuse_opt_free_args(&args);
	free(mfsopts.masterhost);
	free(mfsopts.masterport);
	free(mfsopts.subfolder);
	return res;
}
