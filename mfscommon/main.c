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

#ifndef MFSMAXFILES
#define MFSMAXFILES 5000
#endif

#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/time.h>
#include <signal.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <syslog.h>
#include <string.h>
#include <time.h>
#include <sys/resource.h>
#include <grp.h>
#include <pwd.h>

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)

#include "cfg.h"
#include "main.h"
#include "init.h"

typedef struct deentry {
	void (*fun)(void);
	struct deentry *next;
} deentry;

static deentry *dehead=NULL;


typedef struct weentry {
	void (*fun)(void);
	struct weentry *next;
} weentry;

static weentry *wehead=NULL;


typedef struct ceentry {
	int (*fun)(void);
	struct ceentry *next;
} ceentry;

static ceentry *cehead=NULL;


typedef struct rlentry {
	void (*fun)(void);
	struct rlentry *next;
} rlentry;

static rlentry *rlhead=NULL;


typedef struct pollentry {
	void (*desc)(struct pollfd *,uint32_t *);
	void (*serve)(struct pollfd *);
	struct pollentry *next;
} pollentry;

static pollentry *pollhead=NULL;


typedef struct eloopentry {
	void (*fun)(void);
	struct eloopentry *next;
} eloopentry;

static eloopentry *eloophead=NULL;


typedef struct timeentry {
	time_t nextevent;
	uint32_t seconds;
	int mode;
//	int offset;
	void (*fun)(void);
	struct timeentry *next;
} timeentry;

static timeentry *timehead=NULL;

static int now;
static uint64_t usecnow;
//static int alcnt=0;

static int terminate=0;
static int sigchld=0;
static int reload=0;

/* interface */

void main_destructregister (void (*fun)(void)) {
	deentry *aux=(deentry*)malloc(sizeof(deentry));
	aux->fun = fun;
	aux->next = dehead;
	dehead = aux;
}

void main_canexitregister (int (*fun)(void)) {
	ceentry *aux=(ceentry*)malloc(sizeof(ceentry));
	aux->fun = fun;
	aux->next = cehead;
	cehead = aux;
}

void main_wantexitregister (void (*fun)(void)) {
	weentry *aux=(weentry*)malloc(sizeof(weentry));
	aux->fun = fun;
	aux->next = wehead;
	wehead = aux;
}

void main_reloadregister (void (*fun)(void)) {
	rlentry *aux=(rlentry*)malloc(sizeof(rlentry));
	aux->fun = fun;
	aux->next = rlhead;
	rlhead = aux;
}

void main_pollregister (void (*desc)(struct pollfd *,uint32_t *),void (*serve)(struct pollfd *)) {
	pollentry *aux=(pollentry*)malloc(sizeof(pollentry));
	aux->desc = desc;
	aux->serve = serve;
	aux->next = pollhead;
	pollhead = aux;
}

void main_eachloopregister (void (*fun)(void)) {
	eloopentry *aux=(eloopentry*)malloc(sizeof(eloopentry));
	aux->fun = fun;
	aux->next = eloophead;
	eloophead = aux;
}

void main_timeregister (int mode,uint32_t seconds,uint32_t offset,void (*fun)(void)) {
	timeentry *aux;
	if (seconds==0 || offset>=seconds) return;
	aux = (timeentry*)malloc(sizeof(timeentry));
	aux->nextevent = ((now / seconds) * seconds) + offset;
	while (aux->nextevent<now) {
		aux->nextevent+=seconds;
	}
	aux->seconds = seconds;
	aux->mode = mode;
	aux->fun = fun;
	aux->next = timehead;
	timehead = aux;
}

/* internal */


int canexit() {
	ceentry *aux;
	for (aux = cehead ; aux!=NULL ; aux=aux->next ) {
		if (aux->fun()==0) {
			return 0;
		}
	}
	return 1;
}

void termhandle(int signo) {
	(void)signo;
	terminate=1;
}

void reloadhandle(int signo) {
	(void)signo;
	reload=1;
}

void sigchldhandle(int signo) {
	(void)signo;
	sigchld=1;
}

int main_time() {
	return now;
}

uint64_t main_utime() {
	return usecnow;
}

void destruct() {
	deentry *deit;
	for (deit = dehead ; deit!=NULL ; deit=deit->next ) {
		deit->fun();
	}
}

void mainloop() {
	struct timeval tv;
	pollentry *pollit;
	eloopentry *eloopit;
	timeentry *timeit;
	ceentry *ceit;
	weentry *weit;
	rlentry *rlit;
	struct pollfd pdesc[MFSMAXFILES];
	uint32_t ndesc;
	int i;

	while (terminate!=3) {
		tv.tv_sec=0;
		tv.tv_usec=300000;
		ndesc=0;
		for (pollit = pollhead ; pollit != NULL ; pollit = pollit->next) {
			pollit->desc(pdesc,&ndesc);
		}
		i = poll(pdesc,ndesc,50);
		gettimeofday(&tv,NULL);
		usecnow = tv.tv_sec;
		usecnow *= 1000000;
		usecnow += tv.tv_usec;
		now = tv.tv_sec;
		if (i<0) {
			if (errno==EAGAIN) {
				syslog(LOG_WARNING,"poll returned EAGAIN");
				usleep(100000);
				continue;
			}
			if (errno!=EINTR) {
				syslog(LOG_WARNING,"poll error: %m");
				break;
			}
		} else {
			for (pollit = pollhead ; pollit != NULL ; pollit = pollit->next) {
				pollit->serve(pdesc);
			}
		}
		for (eloopit = eloophead ; eloopit != NULL ; eloopit = eloopit->next) {
			eloopit->fun();
		}
		for (timeit = timehead ; timeit != NULL ; timeit = timeit->next) {
			if (timeit->mode==TIMEMODE_RUNALL) {
				while (now>=timeit->nextevent) {
					timeit->nextevent += timeit->seconds;
					timeit->fun();
				}
			} else if (timeit->mode==TIMEMODE_RUNONCE) {
				if (now>=timeit->nextevent) {
					while (now>=timeit->nextevent) {
						timeit->nextevent += timeit->seconds;
					}
					timeit->fun();
				}
			} else { /* timeit->mode == TIMEMODE_SKIP */
				if (now>=timeit->nextevent) {
					if (now==timeit->nextevent) {
						timeit->fun();
					}
					while (now>=timeit->nextevent) {
						timeit->nextevent += timeit->seconds;
					}
				}
			}
		}
		if (terminate==0 && reload) {
			for (rlit = rlhead ; rlit!=NULL ; rlit=rlit->next ) {
				rlit->fun();
			}
			reload=0;
		}
		if (terminate==1) {
			for (weit = wehead ; weit!=NULL ; weit=weit->next ) {
				weit->fun();
			}
			terminate=2;
		}
		if (terminate==2) {
			i = 1;
			for (ceit = cehead ; ceit!=NULL && i ; ceit=ceit->next ) {
				if (ceit->fun()==0) {
					i=0;
				}
			}
			if (i) {
				terminate=3;
			}
		}
	}
}

int initialize() {
	uint32_t i;
	int ok;
	ok = 1;
	now = time(NULL);
	for (i=0 ; (long int)(RunTab[i].fn)!=0 && ok ; i++) {
		if (RunTab[i].fn()<0) {
			syslog(LOG_ERR,"init: %s failed !!!",RunTab[i].name);
#ifndef NODAEMON
			fprintf(stderr,"init: %s failed !!!",RunTab[i].name);
#endif
			ok=0;
		}
	}
	return ok;
}

static int termsignal[]={
	SIGTERM,
	-1};

static int reloadsignal[]={
	SIGHUP,
	-1};

static int ignoresignal[]={
	SIGINT,
	SIGQUIT,
#ifdef SIGPIPE
	SIGPIPE,
#endif
#ifdef SIGTSTP
	SIGTSTP,
#endif
#ifdef SIGTTIN
	SIGTTIN,
#endif
#ifdef SIGTTOU
	SIGTTOU,
#endif
#ifdef SIGINFO
	SIGINFO,
#endif
#ifdef SIGUSR1
	SIGUSR1,
#endif
#ifdef SIGUSR2
	SIGUSR2,
#endif
#ifdef SIGCHLD
	SIGCHLD,
#endif
#ifdef SIGCLD
	SIGCLD,
#endif
	-1};

void set_signal_handlers(void) {
	struct sigaction sa;
	uint32_t i;

	sa.sa_flags = SA_RESTART;
	sigemptyset(&sa.sa_mask);

	sa.sa_handler = termhandle;
	for (i=0 ; termsignal[i]>0 ; i++) {
		sigaction(termsignal[i],&sa,(struct sigaction *)0);
	}
	sa.sa_handler = reloadhandle;
	for (i=0 ; reloadsignal[i]>0 ; i++) {
		sigaction(reloadsignal[i],&sa,(struct sigaction *)0);
	}
	sa.sa_handler = SIG_IGN;
	for (i=0 ; ignoresignal[i]>0 ; i++) {
		sigaction(ignoresignal[i],&sa,(struct sigaction *)0);
	}
}

void changeugid(void) {
	char *wuser;
	char *wgroup;
	uid_t wrk_uid;
	gid_t wrk_gid;
	int gidok;

	if (geteuid()==0) {
		config_getnewstr("WORKING_USER",DEFAULT_USER,&wuser);
		config_getnewstr("WORKING_GROUP",DEFAULT_GROUP,&wgroup);

		gidok = 0;
		wrk_gid = -1;
		if (wgroup[0]=='#') {
			wrk_gid = strtol(wgroup+1,NULL,10);
			gidok = 1;
		} else if (wgroup[0]) {
			struct group *gr;
			gr = getgrnam(wgroup);
			if (gr==NULL) {
				fprintf(stderr,"%s: no such group !!!\n",wgroup);
				syslog(LOG_WARNING,"%s: no such group !!!",wgroup);
				exit(1);
			} else {
				wrk_gid = gr->gr_gid;
				gidok = 1;
			}
		}

		if (wuser[0]=='#') {
			struct passwd *pw;
			wrk_uid = strtol(wuser+1,NULL,10);
			if (gidok==0) {
				pw = getpwuid(wrk_uid);
				if (pw==NULL) {
					fprintf(stderr,"%s: no such user id - can't obtain group id\n",wuser+1);
					syslog(LOG_ERR,"%s: no such user id - can't obtain group id",wuser+1);
					exit(1);
				}
				wrk_gid = pw->pw_gid;
			}
		} else {
			struct passwd *pw;
			pw = getpwnam(wuser);
			if (pw==NULL) {
				fprintf(stderr,"%s: no such user !!!\n",wuser);
				syslog(LOG_ERR,"%s: no such user !!!",wuser);
				exit(1);
			}
			wrk_uid = pw->pw_uid;
			if (gidok==0) {
				wrk_gid = pw->pw_gid;
			}
		}
		free(wuser);
		free(wgroup);

		setgid(wrk_gid);
		syslog(LOG_NOTICE,"set gid to %d",(int)wrk_gid);
		setuid(wrk_uid);
		syslog(LOG_NOTICE,"set uid to %d",(int)wrk_uid);
	}
}

int wdlock() {
	int lfp;
	uint8_t l;
	struct stat sb;
	lfp=open(".lock_" STR(APPNAME),O_RDWR|O_CREAT|O_TRUNC,0640);
	if (lfp<0) {
		syslog(LOG_ERR,"can't create lock file in working directory: %m");
		return -1;
	}
	l=0;
	while (lockf(lfp,F_TLOCK,0)<0) {
		if (errno!=EAGAIN) {
			syslog(LOG_ERR,"lock error: %m");
			exit(1);
		}
		sleep(1);
		l++;
		if (l>3) {
			syslog(LOG_ERR,"working directory already locked (used by another instance of the same mfs process)");
			exit(1);
		}
	}
	if (fstat(lfp,&sb)<0) {
		syslog(LOG_NOTICE,"working directory lock file fstat error: %m");
		return -1;
	}
	return 0;
}

void justlock(const char *lockfname) {
	int lfp,s;
	char str[13];
	lfp=open(lockfname,O_RDWR|O_CREAT|O_TRUNC,0640);
	if (lfp<0) {
		syslog(LOG_ERR,"open %s error: %m",lockfname);
		exit(1); /* can not open */
	}
	if (lockf(lfp,F_TLOCK,0)<0) {
		if (errno==EAGAIN) {
			syslog(LOG_NOTICE,"'%s' is locked",lockfname);
			exit(0);
		}
		syslog(LOG_ERR,"lock %s error: %m",lockfname);
		exit(1); /* can not lock */ 
	}
	/* first instance continues */
	s = snprintf(str,13,"%d\n",(int)getpid());
	if (s>=13) {
		syslog(LOG_ERR,"can't write pid to lockfile %s: %m",lockfname);
		exit(1); /* can not write */
	}
	/* record pid to lockfile */ 
	if (write(lfp,str,s)!=s) {
		syslog(LOG_ERR,"can't write pid to lockfile %s: %m",lockfname);
		exit(1); /* can not write */
	}
}

void killprevious(const char *lockfname) {
	int lfp,s;
	char str[13];
	uint32_t l;
	pid_t ptk;
	lfp=open(lockfname,O_RDWR);
	if (lfp<0) {
		if (errno==ENOENT) {    // no lock file ?
			justlock(lockfname);     // so make new one
			return;
		}       
		syslog(LOG_ERR,"open %s error: %m",lockfname);
		exit(1);
	}
	if (lockf(lfp,F_TLOCK,0)<0) {
		if (errno!=EAGAIN) {
			syslog(LOG_ERR,"lock %s error: %m",lockfname);
			exit(1);
		}
		l=read(lfp,str,13);
		if (l==0 || l>=13) {
			syslog(LOG_ERR,"wrong pid in lockfile %s",lockfname);
			exit(1);
		}
		str[l]=0;
		ptk = strtol(str,NULL,10);
		if (kill(ptk,SIGTERM)<0) {
			syslog(LOG_WARNING,"can't kill previous process (%m).");
			exit(1);
		}
		l=0;
		while (lockf(lfp,F_TLOCK,0)<0) {
			if (errno!=EAGAIN) {
				syslog(LOG_ERR,"lock %s error: %m",lockfname);
				exit(1);
			}
			sleep(1);
			l++;
			if (l%10==0) {
				syslog(LOG_WARNING,"about %"PRIu32" seconds passed and '%s' is still locked",l,lockfname);
			}
		}
	}
	s = snprintf(str,13,"%d\n",(int)getpid());
	if (s>=13) {
		syslog(LOG_ERR,"can't write pid to lockfile %s: %m",lockfname);
		exit(1); /* can not write */
	}
	/* record pid to lockfile */ 
	lseek(lfp,0,SEEK_SET);
	if (ftruncate(lfp,0)!=0) {
		syslog(LOG_ERR,"can't write pid to lockfile %s: %m",lockfname);
		exit(1); /* can not write */
	}
	if (write(lfp,str,s)!=s) {
		syslog(LOG_ERR,"can't write pid to lockfile %s: %m",lockfname);
		exit(1); /* can not write */
	}
}

void makedaemon() {
	int nd,f;
	f = fork();
	if (f<0) {
		syslog(LOG_ERR,"first fork error: %m");
		exit(1);
	}
	if (f>0) {
		exit(0);
	}
	setsid();
	setpgid(0,getpid());
	f = fork();
	if (f<0) {
		syslog(LOG_ERR,"second fork error: %m");
		exit(1);
	}
	if (f>0) {
		exit(0);
	}
	set_signal_handlers();
#ifdef HAVE_DUP2
	if ((nd = open("/dev/null", O_RDWR, 0)) != -1) {
		dup2(nd, STDIN_FILENO);
		dup2(nd, STDOUT_FILENO);
		dup2(nd, STDERR_FILENO);
		if (nd!=STDIN_FILENO && nd!=STDOUT_FILENO && nd!=STDERR_FILENO) {
			close (nd);
		}
	} else {
		syslog(LOG_ERR,"can't open /dev/null (%m)");
		exit(1);
	}
#else
	close(STDIN_FILENO);
	close(STDOUT_FILENO);
	close(STDERR_FILENO);
	nd = open("/dev/null", O_RDWR, 0);
	if (nd!=STDIN_FILENO) {
		exit(1);
	}
	if (dup(nd)!=STDOUT_FILENO) {
		exit(1);
	}
	if (dup(nd)!=STDERR_FILENO) {
		exit(1);
	}
#endif
}

void createpath(const char *filename) {
	char pathbuff[1024];
	const char *src = filename;
	char *dst = pathbuff;
	if (*src=='/') *dst++=*src++;

	while (*src) {
		while (*src!='/' && *src) {
			*dst++=*src++;
		}
		if (*src=='/') {
			*dst='\0';
			if (mkdir(pathbuff,(mode_t)0777)<0) {
				if (errno!=EEXIST) {
					syslog(LOG_NOTICE,"creating directory %s: %m",pathbuff);
				}
			} else {
				syslog(LOG_NOTICE,"created directory %s",pathbuff);
			}
			*dst++=*src++;
		}
	}
}

int main(int argc,char **argv) {
	char *logappname;
	char *lockfname;
	char *wrkdir;
	char *cfgfile;
	int ch;
	int run,killold,rundaemon;
	struct rlimit rls;
	
	cfgfile=strdup(ETC_PATH "/" STR(APPNAME) ".cfg");
	rundaemon=1;
	killold=1;
	run=1;

	while ((ch = getopt(argc, argv, "vdfsc:h?")) != -1) {
		switch(ch) {
			case 'v':
				printf("version: %u.%u.%u\n",VERSMAJ,VERSMID,VERSMIN);
				return 0;
			case 'd':
				rundaemon=0;
				break;
			case 'f':
				killold=0;
				break;
			case 's':
				run=0;
				break;
			case 'c':
				free(cfgfile);
				cfgfile = strdup(optarg);
				break;
			default:
				printf("usage: %s [-f] [-s] [-c cfgfile]\n\n-f : run without killing old process (exit if lock exists)\n-s : kill old process only\n-c cfgfile : use given config file\n",argv[0]);
				return 0;
		}
	}

	if (config_load(cfgfile)==0) {
		fprintf(stderr,"can't load config file: %s\n",cfgfile);
		return 1;
	}
	free(cfgfile);

	config_getnewstr("SYSLOG_IDENT",STR(APPNAME),&logappname);

	if (rundaemon) {
		if (logappname[0]) {
			openlog(logappname, LOG_PID | LOG_NDELAY , LOG_DAEMON);
		} else {
			openlog(STR(APPNAME), LOG_PID | LOG_NDELAY , LOG_DAEMON);
		}
	} else {
#if defined(LOG_PERROR)
		if (logappname[0]) {
			openlog(logappname, LOG_PID | LOG_NDELAY | LOG_PERROR, LOG_USER);
		} else {
			openlog(STR(APPNAME), LOG_PID | LOG_NDELAY | LOG_PERROR, LOG_USER);
		}
#else
		if (logappname[0]) {
			openlog(logappname, LOG_PID | LOG_NDELAY, LOG_USER);
		} else {
			openlog(STR(APPNAME), LOG_PID | LOG_NDELAY, LOG_USER);
		}
#endif
	}

	rls.rlim_cur = MFSMAXFILES;
	rls.rlim_max = MFSMAXFILES;
	if (setrlimit(RLIMIT_NOFILE,&rls)<0) {
		fprintf(stderr,"can't change open files limit\n");
		syslog(LOG_WARNING,"can't change open files limit");
	}

	changeugid();

	config_getnewstr("DATA_PATH",DATA_PATH,&wrkdir);

	if (chdir(wrkdir)<0) {
		fprintf(stderr,"can't set working directory to %s\n",wrkdir);
		syslog(LOG_ERR,"can't set working directory to %s",wrkdir);
		return 1;
	}

	if (rundaemon) {
		makedaemon();
	}

	umask(027);

	config_getnewstr("LOCK_FILE",RUN_PATH "/" STR(APPNAME) ".lock",&lockfname);
	createpath(lockfname);

	if (killold) {
		killprevious(lockfname);
	} else {
		justlock(lockfname);
	}

	if (wdlock()<0) {
		return 1;
	}

	if (run==0) {
		return 0;
	}

	if  (initialize()) {
		if (getrlimit(RLIMIT_NOFILE,&rls)==0) {
			syslog(LOG_NOTICE,"open files limit: %lu",(unsigned long)(rls.rlim_cur));
		}
		mainloop();
	}
	destruct();

	closelog();
	return 0;
}
