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
#include <grp.h>
#include <pwd.h>

#include "conf.h"
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


typedef struct selentry {
	int (*desc)(fd_set *,fd_set *);
	void (*serve)(fd_set *,fd_set *);
	struct selentry *next;
} selentry;

static selentry *selhead=NULL;


typedef struct eloopentry {
	void (*fun)(void);
	struct eloopentry *next;
} eloopentry;

static eloopentry *eloophead=NULL;


typedef struct timeentry {
	time_t nextevent;
	int seconds;
//	int offset;
	void (*fun)(void);
	struct timeentry *next;
} timeentry;

static timeentry *timehead=NULL;

static int now;
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

void main_selectregister (int (*desc)(fd_set *,fd_set *),void (*serve)(fd_set *,fd_set *)) {
	selentry *aux=(selentry*)malloc(sizeof(selentry));
	aux->desc = desc;
	aux->serve = serve;
	aux->next = selhead;
	selhead = aux;
}

void main_eachloopregister (void (*fun)(void)) {
	eloopentry *aux=(eloopentry*)malloc(sizeof(eloopentry));
	aux->fun = fun;
	aux->next = eloophead;
	eloophead = aux;
}

void main_timeregister (int seconds,int offset,void (*fun)(void)) {
	timeentry *aux;
	if (seconds<1) return;
	if (offset>=seconds || offset<0) return;
	aux = (timeentry*)malloc(sizeof(timeentry));
	aux->nextevent = ((now / seconds) * seconds) + offset + seconds;
	aux->seconds = seconds;
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

void destruct() {
	deentry *deit;
	for (deit = dehead ; deit!=NULL ; deit=deit->next ) {
		deit->fun();
	}
}

void mainloop() {
	struct timeval tv;
	selentry *selit;
	eloopentry *eloopit;
	timeentry *timeit;
	deentry *deit;
	ceentry *ceit;
	weentry *weit;
	rlentry *rlit;
	fd_set rset,wset;
	int max;
	int i;

	while (terminate!=3) {
		tv.tv_sec=0;
		tv.tv_usec=300000;
		FD_ZERO(&rset);
		FD_ZERO(&wset);
		max = -1;
		for (selit = selhead ; selit != NULL ; selit = selit->next) {
			i = selit->desc(&rset,&wset);
			if (i>max) max=i;
		}
//		if (max==-1 && terminate==0) {
//			terminate=1;
//		}
		i=select(max+1,&rset,&wset,NULL,&tv);
		now = time(NULL);
		if (i<0) {
			if (errno!=EINTR) {
				syslog(LOG_WARNING,"select error: %m");
				break;
			}
		} else {
			for (selit = selhead ; selit != NULL ; selit = selit->next) {
				selit->serve(&rset,&wset);
			}
		}
		for (eloopit = eloophead ; eloopit != NULL ; eloopit = eloopit->next) {
			eloopit->fun();
		}
		for (timeit = timehead ; timeit != NULL ; timeit = timeit->next) {
			while (now>=timeit->nextevent) {
				// syslog(LOG_INFO,"cyk (%d)",timeit->seconds);
				timeit->nextevent += timeit->seconds;
				timeit->fun();
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
	for (deit = dehead ; deit!=NULL ; deit=deit->next ) {
		deit->fun();
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

	if (geteuid()==0) {
		config_getnewstr("WORKING_USER",DEFAULT_USER,&wuser);
		config_getnewstr("WORKING_GROUP",DEFAULT_GROUP,&wgroup);

		if (wgroup[0]=='#') {
			wrk_gid = strtol(wgroup+1,NULL,10);
		} else {
			struct group *gr;
			gr = getgrnam(wgroup);
			if (gr==NULL) {
				fprintf(stderr,"%s: no such group !!!\n",wgroup);
				syslog(LOG_WARNING,"%s: no such group !!!\n",wgroup);
				exit(1);
			}
			wrk_gid = gr->gr_gid;
		}

		if (wuser[0]=='#') {
			wrk_uid = strtol(wuser+1,NULL,10);
		} else {
			struct passwd *pw;
			pw = getpwnam(wuser);
			if (pw==NULL) {
				fprintf(stderr,"%s: no such user !!!\n",wuser);
				syslog(LOG_WARNING,"%s: no such user !!!\n",wuser);
				exit(1);
			}
			wrk_uid = pw->pw_uid;
		}
		free(wuser);
		free(wgroup);

		setgid(wrk_gid);
		syslog(LOG_NOTICE,"set gid to %d",wrk_gid);
		setuid(wrk_uid);
		syslog(LOG_NOTICE,"set uid to %d",wrk_uid);
	}
}

void justlock(const char *lockfname) {
	int lfp;
	char str[10];
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
	sprintf(str,"%d\n",(int)getpid()); 
	write(lfp,str,strlen(str)); /* record pid to lockfile */ 
}

void killprevious(const char *lockfname) {
	int lfp;
	char str[10];
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
	read(lfp,str,10);
	str[9]=0;
	ptk = strtol(str,NULL,10);
	if (kill(ptk,SIGTERM)<0) {
		syslog(LOG_WARNING,"can't kill previous process (%m).");
		justlock(lockfname);     // so make new lock file
		return;
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
			syslog(LOG_WARNING,"about %u seconds passed and '%s' is still locked",l,lockfname);
		}
	}
	sprintf(str,"%d\n",(int)getpid());
	lseek(lfp,0,SEEK_SET);
	ftruncate(lfp,0);
	write(lfp,str,strlen(str)); /* record pid to lockfile */
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
/* no dup2 ?
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
*/
}

int main(int argc,char **argv) {
	char *logappname;
	char *lockfname;
	char *wrkdir;
	char *cfgfile;
	char ch;
	int run,killold,rundaemon;
	
	cfgfile=strdup(ETC_PATH "/" STR(APPNAME) ".cfg");
	rundaemon=1;
	killold=1;
	run=1;

	while ((ch = getopt(argc, argv, "dfsc:h?")) != -1) {
		switch(ch) {
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
				printf("usage: %s [-f] [-s] [-c cfgfile]\n\n-f : run without killing old process (exit if lock exists)\n-s : kill old process only\n-c cfgfile : use given config file",argv[0]);
				return 0;
		}
	}

	if (config_load(cfgfile)==0) {
		fprintf(stderr,"can't load config file: %s\n",cfgfile);
		return -1;
	}

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


	changeugid();

	if (rundaemon) {
		makedaemon();
	}

	config_getnewstr("DATA_PATH",DATA_PATH,&wrkdir);

	if (chdir(wrkdir)<0) {
		chdir("/");
	}

	umask(027);

	config_getnewstr("LOCK_FILE",RUN_PATH "/" STR(APPNAME) ".lock",&lockfname);

	if (killold) {
		killprevious(lockfname);
	} else {
		justlock(lockfname);
	}

	if (run==0) {
		return 0;
	}

	if  (initialize()) {
		mainloop();
	} else {
		destruct();
	}

	closelog();
	return 0;
}
