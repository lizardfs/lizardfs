/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"
#include "init.h"

#ifndef MFSMAXFILES
#define MFSMAXFILES 5000
#endif

#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <pwd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <syslog.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <list>
#include <memory>

#include "common/cfg.h"
#include "common/crc.h"
#include "common/cwrap.h"
#include "common/exceptions.h"
#include "common/exit_status.h"
#include "common/main.h"
#include "common/massert.h"
#include "common/mfserr.h"
#include "common/setup.h"
#include "common/slogger.h"

#if defined(LIZARDFS_HAVE_MLOCKALL)
#  if defined(LIZARDFS_HAVE_SYS_MMAN_H)
#    include <sys/mman.h>
#  endif
#  if defined(LIZARDFS_HAVE_SYS_RESOURCE_H)
#    include <sys/resource.h>
#  endif
#  if defined(RLIMIT_MEMLOCK) && defined(MCL_CURRENT) && defined(MCL_FUTURE)
#    define MFS_USE_MEMLOCK 1
#  endif
#endif

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)

enum class RunMode {
	kRestart = 0,
	kStart = 1,
	kStop = 2,
	kReload = 3,
	kTest = 4,
	kKill = 5,
	kIsAlive = 6
};

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


struct timeentry {
	typedef void (*fun_t)(void);
	timeentry(uint32_t ne, uint32_t sec, uint32_t off, int mod, fun_t f)
		: nextevent(ne), seconds(sec), offset(off), mode(mod), fun(f) {
	}
	uint32_t nextevent;
	uint32_t seconds;
	uint32_t offset;
	int mode;
	fun_t fun;
};

typedef std::list<timeentry> TimeEntries;
namespace {
TimeEntries gTimeEntries;
}

static uint32_t now;
static uint64_t usecnow;
static bool gRunAsDaemon = true;

static int signalpipe[2];

/* interface */

void main_destructregister (void (*fun)(void)) {
	deentry *aux=(deentry*)malloc(sizeof(deentry));
	passert(aux);
	aux->fun = fun;
	aux->next = dehead;
	dehead = aux;
}

void main_canexitregister (int (*fun)(void)) {
	ceentry *aux=(ceentry*)malloc(sizeof(ceentry));
	passert(aux);
	aux->fun = fun;
	aux->next = cehead;
	cehead = aux;
}

void main_wantexitregister (void (*fun)(void)) {
	weentry *aux=(weentry*)malloc(sizeof(weentry));
	passert(aux);
	aux->fun = fun;
	aux->next = wehead;
	wehead = aux;
}

void main_reloadregister (void (*fun)(void)) {
	rlentry *aux=(rlentry*)malloc(sizeof(rlentry));
	passert(aux);
	aux->fun = fun;
	aux->next = rlhead;
	rlhead = aux;
}

void main_pollregister (void (*desc)(struct pollfd *,uint32_t *),void (*serve)(struct pollfd *)) {
	pollentry *aux=(pollentry*)malloc(sizeof(pollentry));
	passert(aux);
	aux->desc = desc;
	aux->serve = serve;
	aux->next = pollhead;
	pollhead = aux;
}

void main_eachloopregister (void (*fun)(void)) {
	eloopentry *aux=(eloopentry*)malloc(sizeof(eloopentry));
	passert(aux);
	aux->fun = fun;
	aux->next = eloophead;
	eloophead = aux;
}

void* main_timeregister (int mode,uint32_t seconds,uint32_t offset,void (*fun)(void)) {
	if (seconds==0 || offset>=seconds) {
		return NULL;
	}
	uint32_t nextevent = ((now / seconds) * seconds) + offset;
	while (nextevent<now) {
		nextevent+=seconds;
	}
	gTimeEntries.push_front(timeentry(nextevent, seconds, offset, mode, fun));
	return &gTimeEntries.front();
}

void main_timeunregister(void* handler) {
	TimeEntries::iterator it(gTimeEntries.begin());
	for (TimeEntries::iterator end(gTimeEntries.end()); it != end; ++it) {
		if (&(*it) == handler) {
			gTimeEntries.erase(it);
			break;
		}
	}
	massert(it != gTimeEntries.end(), "unregistering unknown handle from time table");
}

int main_timechange(void* x,int mode,uint32_t seconds,uint32_t offset) {
	timeentry *aux = (timeentry*)x;
	if (seconds==0 || offset>=seconds) {
		return -1;
	}
	aux->nextevent = ((now / seconds) * seconds) + offset;
	while (aux->nextevent<now) {
		aux->nextevent+=seconds;
	}
	aux->seconds = seconds;
	aux->offset = offset;
	aux->mode = mode;
	return 0;
}

/* internal */

void free_all_registered_entries(void) {
	deentry *de,*den;
	ceentry *ce,*cen;
	weentry *we,*wen;
	rlentry *re,*ren;
	pollentry *pe,*pen;
	eloopentry *ee,*een;

	for (de = dehead ; de ; de = den) {
		den = de->next;
		free(de);
	}

	for (ce = cehead ; ce ; ce = cen) {
		cen = ce->next;
		free(ce);
	}

	for (we = wehead ; we ; we = wen) {
		wen = we->next;
		free(we);
	}

	for (re = rlhead ; re ; re = ren) {
		ren = re->next;
		free(re);
	}

	for (pe = pollhead ; pe ; pe = pen) {
		pen = pe->next;
		free(pe);
	}

	for (ee = eloophead ; ee ; ee = een) {
		een = ee->next;
		free(ee);
	}

	gTimeEntries.clear();
}

int canexit() {
	ceentry *aux;
	for (aux = cehead ; aux!=NULL ; aux=aux->next) {
		if (aux->fun()==0) {
			return 0;
		}
	}
	return 1;
}

uint32_t main_time() {
	return now;
}

uint64_t main_utime() {
	return usecnow;
}

void destruct() {
	deentry *deit;
	for (deit = dehead ; deit!=NULL ; deit=deit->next) {
		deit->fun();
	}
}

void mainloop() {
	uint32_t prevtime = 0;
	struct timeval tv;
	pollentry *pollit;
	eloopentry *eloopit;
	ceentry *ceit;
	weentry *weit;
	rlentry *rlit;
	struct pollfd pdesc[MFSMAXFILES];
	uint32_t ndesc;
	int i;
	int t,r;

	t = 0;
	r = 0;
	while (t!=3) {
		ndesc=1;
		pdesc[0].fd = signalpipe[0];
		pdesc[0].events = POLLIN;
		pdesc[0].revents = 0;
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
				syslog(LOG_WARNING,"poll error: %s",strerr(errno));
				break;
			}
		} else {
			if ((pdesc[0].revents)&POLLIN) {
				uint8_t sigid;
				if (read(signalpipe[0],&sigid,1)==1) {
					if (sigid=='\001' && t==0) {
						syslog(LOG_NOTICE,"terminate signal received");
						t = 1;
					} else if (sigid=='\002') {
						syslog(LOG_NOTICE,"reloading config files");
						r = 1;
					} else if (sigid=='\003') {
						syslog(LOG_NOTICE, "Received SIGUSR1, killing gently...");
						exit(LIZARDFS_EXIT_STATUS_GENTLY_KILL);
					}
				}
			}
			for (pollit = pollhead ; pollit != NULL ; pollit = pollit->next) {
				pollit->serve(pdesc);
			}
		}
		for (eloopit = eloophead ; eloopit != NULL ; eloopit = eloopit->next) {
			eloopit->fun();
		}
		if (now<prevtime) {
			// time went backward !!! - recalculate "nextevent" time
			// adding previous_time_to_run prevents from running next event too soon.
			for (timeentry& timeit : gTimeEntries) {
				uint32_t previous_time_to_run = timeit.nextevent - prevtime;
				if (previous_time_to_run > timeit.seconds) {
					previous_time_to_run = timeit.seconds;
				}
				timeit.nextevent = ((now / timeit.seconds) * timeit.seconds) + timeit.offset;
				while (timeit.nextevent <= now+previous_time_to_run) {
					timeit.nextevent += timeit.seconds;
				}
			}
		} else if (now>prevtime+3600) {
			// time went forward !!! - just recalculate "nextevent" time
			for (timeentry& timeit : gTimeEntries) {
				timeit.nextevent = ((now / timeit.seconds) * timeit.seconds) + timeit.offset;
				while (now >= timeit.nextevent) {
					timeit.nextevent += timeit.seconds;
				}
			}
		}
		for (timeentry& timeit : gTimeEntries) {
			if (now >= timeit.nextevent) {
				if (timeit.mode == TIMEMODE_RUN_LATE) {
					while (now >= timeit.nextevent) {
						timeit.nextevent += timeit.seconds;
					}
					timeit.fun();
				} else { /* timeit.mode == TIMEMODE_SKIP_LATE */
					if (now == timeit.nextevent) {
						timeit.fun();
					}
					while (now >= timeit.nextevent) {
						timeit.nextevent += timeit.seconds;
					}
				}
			}
		}
		prevtime = now;
		if (t==0 && r) {
			cfg_reload();
			for (rlit = rlhead ; rlit!=NULL ; rlit=rlit->next) {
				rlit->fun();
			}
			r = 0;
		}
		if (t==1) {
			for (weit = wehead ; weit!=NULL ; weit=weit->next) {
				weit->fun();
			}
			t = 2;
		}
		if (t==2) {
			i = 1;
			for (ceit = cehead ; ceit!=NULL && i ; ceit=ceit->next) {
				if (ceit->fun()==0) {
					i=0;
				}
			}
			if (i) {
				t = 3;
			}
		}
	}
}

int initialize(void) {
	uint32_t i;
	int ok;
	ok = 1;
	for (i=0 ; (long int)(RunTab[i].fn)!=0 && ok ; i++) {
		now = time(NULL);
		try {
			if (RunTab[i].fn() < 0) {
				mfs_arg_syslog(LOG_ERR, "init: %s failed !!!", RunTab[i].name);
				ok=0;
			}
		} catch (const std::exception& e) {
			mfs_arg_syslog(LOG_ERR, "init: %s failed: %s !!!", RunTab[i].name, e.what());
			ok = 0;
		}
	}
	return ok;
}

int initialize_late(void) {
	uint32_t i;
	int ok;
	ok = 1;
	for (i=0 ; (long int)(LateRunTab[i].fn)!=0 && ok ; i++) {
		now = time(NULL);
		if (LateRunTab[i].fn()<0) {
			mfs_arg_syslog(LOG_ERR,"init: %s failed !!!",RunTab[i].name);
			ok=0;
		}
	}
	now = time(NULL);
	return ok;
}

const std::string& set_syslog_ident() {
	static std::string logIdent;
	logIdent = cfg_get("SYSLOG_IDENT", std::string(STR(APPNAME)));
	if (logIdent.empty()) {
		logIdent = STR(APPNAME);
	}
	closelog();
	if (gRunAsDaemon) {
		openlog(logIdent.c_str(), LOG_PID | LOG_NDELAY, LOG_DAEMON);
	} else {
#if defined(LOG_PERROR)
		openlog(logIdent.c_str(), LOG_PID | LOG_NDELAY | LOG_PERROR, LOG_USER);
#else
		openlog(logIdent.c_str(), LOG_PID | LOG_NDELAY, LOG_USER);
#endif
	}
	return logIdent;
}

void main_reload() {
	syslog(LOG_NOTICE, "Changing SYSLOG_IDENT to %s",
			cfg_get("SYSLOG_IDENT", STR(APPNAME)).c_str());
	set_syslog_ident();
}

/* signals */

static int termsignal[]={
	SIGTERM,
	-1
};

static int reloadsignal[]={
	SIGHUP,
	-1
};

static int ignoresignal[]={
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
#if defined(SIGUSR1) && !defined(ENABLE_EXIT_ON_USR1)
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
	-1
};

static int exitsignal[]={
#if defined(SIGUSR1) && defined(ENABLE_EXIT_ON_USR1)
	SIGUSR1,
#endif
	-1
};

static int daemonignoresignal[]={
	SIGINT,
	-1
};

void termhandle(int signo) {
	signo = write(signalpipe[1],"\001",1); // killing two birds with one stone - use signo and do something with value returned by write :)
	(void)signo; // and then use this value to calm down compiler ;)
}

void reloadhandle(int signo) {
	signo = write(signalpipe[1],"\002",1); // see above
	(void)signo;
}

void exithandle(int signo) {
	signo = write(signalpipe[1],"\003",1); // see above
	(void)signo;
}

void set_signal_handlers(int daemonflag) {
	struct sigaction sa;
	uint32_t i;

	zassert(pipe(signalpipe));

#ifdef SA_RESTART
	sa.sa_flags = SA_RESTART;
#else
	sa.sa_flags = 0;
#endif
	sigemptyset(&sa.sa_mask);

	sa.sa_handler = termhandle;
	for (i=0 ; termsignal[i]>0 ; i++) {
		sigaction(termsignal[i],&sa,(struct sigaction *)0);
	}
	sa.sa_handler = reloadhandle;
	for (i=0 ; reloadsignal[i]>0 ; i++) {
		sigaction(reloadsignal[i],&sa,(struct sigaction *)0);
	}
	sa.sa_handler = exithandle;
	for (i=0 ; exitsignal[i]>0 ; i++) {
		sigaction(exitsignal[i],&sa,(struct sigaction *)0);
	}
	sa.sa_handler = SIG_IGN;
	for (i=0 ; ignoresignal[i]>0 ; i++) {
		sigaction(ignoresignal[i],&sa,(struct sigaction *)0);
	}
	sa.sa_handler = daemonflag?SIG_IGN:termhandle;
	for (i=0 ; daemonignoresignal[i]>0 ; i++) {
		sigaction(daemonignoresignal[i],&sa,(struct sigaction *)0);
	}
}

void signal_cleanup(void) {
	close(signalpipe[0]);
	close(signalpipe[1]);
}

void changeugid(void) {
	char pwdgrpbuff[16384];
	struct passwd pwd,*pw;
	struct group grp,*gr;
	char *wuser;
	char *wgroup;
	uid_t wrk_uid;
	gid_t wrk_gid;
	int gidok;

	if (geteuid()==0) {
		wuser = cfg_getstr("WORKING_USER",DEFAULT_USER);
		wgroup = cfg_getstr("WORKING_GROUP",DEFAULT_GROUP);

		gidok = 0;
		wrk_gid = -1;
		if (wgroup[0]=='#') {
			wrk_gid = strtol(wgroup+1,NULL,10);
			gidok = 1;
		} else if (wgroup[0]) {
			getgrnam_r(wgroup,&grp,pwdgrpbuff,16384,&gr);
			if (gr==NULL) {
				mfs_arg_syslog(LOG_WARNING,"%s: no such group !!!",wgroup);
				exit(LIZARDFS_EXIT_STATUS_ERROR);
			} else {
				wrk_gid = gr->gr_gid;
				gidok = 1;
			}
		}

		if (wuser[0]=='#') {
			wrk_uid = strtol(wuser+1,NULL,10);
			if (gidok==0) {
				getpwuid_r(wrk_uid,&pwd,pwdgrpbuff,16384,&pw);
				if (pw==NULL) {
					mfs_arg_syslog(LOG_ERR,"%s: no such user id - can't obtain group id",wuser+1);
					exit(LIZARDFS_EXIT_STATUS_ERROR);
				}
				wrk_gid = pw->pw_gid;
			}
		} else {
			getpwnam_r(wuser,&pwd,pwdgrpbuff,16384,&pw);
			if (pw==NULL) {
				mfs_arg_syslog(LOG_ERR,"%s: no such user !!!",wuser);
				exit(LIZARDFS_EXIT_STATUS_ERROR);
			}
			wrk_uid = pw->pw_uid;
			if (gidok==0) {
				wrk_gid = pw->pw_gid;
			}
		}
		free(wuser);
		free(wgroup);

		if (setgid(wrk_gid)<0) {
			mfs_arg_errlog(LOG_ERR,"can't set gid to %d",(int)wrk_gid);
			exit(LIZARDFS_EXIT_STATUS_ERROR);
		} else {
			syslog(LOG_NOTICE,"set gid to %d",(int)wrk_gid);
		}
		if (setuid(wrk_uid)<0) {
			mfs_arg_errlog(LOG_ERR,"can't set uid to %d",(int)wrk_uid);
			exit(LIZARDFS_EXIT_STATUS_ERROR);
		} else {
			syslog(LOG_NOTICE,"set uid to %d",(int)wrk_uid);
		}
	}
}

class FileLock {
public:
	enum class LockStatus {
		kSuccess = 0,
		kAlive = 1,
		kAgain = 2,
		kFail = -1
	};

	FileLock(RunMode runmode, uint32_t timeout)
			: fd_(),
			  name_("." STR(APPNAME) ".lock"),
			  lockstatus_(LockStatus::kFail),
			  thisProcessCreatedLockFile_(false) {
		while ((lockstatus_ = wdlock(runmode, timeout)) == LockStatus::kAgain) {
		}
		if (lockstatus_ == LockStatus::kFail) {
			throw FilesystemException("");
		}
	}
	virtual ~FileLock() {
		/*
		 * Order of unlock/file deletion matters
		 * for RunMode::kRestart mode.
		 * We need to remove file first and then
		 * unlock file.
		 */
		if (thisProcessCreatedLockFile_) {
			::unlink(name_.c_str());
		}
	}

	LockStatus lockstatus() const {
		return lockstatus_;
	}
	const std::string& name() const {
		return name_;
	}

private:
	LockStatus wdlock(RunMode runmode, uint32_t timeout);
	pid_t mylock();
	void createLockFile();

	FileDescriptor fd_;
	std::string name_;
	LockStatus lockstatus_;
	bool thisProcessCreatedLockFile_;
};

pid_t FileLock::mylock() {
	struct flock fl;
	fl.l_start = 0;
	fl.l_len = 0;
	fl.l_pid = getpid();
	fl.l_type = F_WRLCK;
	fl.l_whence = SEEK_SET;
	pid_t pid = -1;
	for (;;) {
		if (fcntl(fd_.get(), F_SETLK, &fl) >= 0) { // lock set
			pid = 0;                       // ok
			break;
		}
		if (errno!=EAGAIN) {             // error other than "already locked"
			break;
		}
		if (fcntl(fd_.get(), F_GETLK, &fl) < 0) {  // get lock owner
			break;
		}
		if (fl.l_type!=F_UNLCK) {        // found lock
			pid = fl.l_pid;                // return lock owner
			break;
		}
	}
	return pid;
}

void FileLock::createLockFile() {
	bool notExisted((::access(name_.c_str(), F_OK) != 0) && (errno == ENOENT));
	fd_.reset(open(name_.c_str(), O_WRONLY | O_CREAT, 0666));
	if (!fd_.isOpened()) {
		throw FilesystemException("can't create lockfile in working directory");
	}
	thisProcessCreatedLockFile_ = notExisted;
}

FileLock::LockStatus FileLock::wdlock(RunMode runmode, uint32_t timeout) {
	createLockFile();
	pid_t ownerpid(mylock());
	if (ownerpid<0) {
		mfs_errlog(LOG_ERR,"fcntl error");
		return LockStatus::kFail;
	}
	if (ownerpid>0) {
		if (runmode==RunMode::kIsAlive) {
			return LockStatus::kAlive;
		}
		if (runmode==RunMode::kTest) {
			fprintf(stderr,STR(APPNAME) " pid: %ld\n",(long)ownerpid);
			return LockStatus::kFail;
		}
		if (runmode==RunMode::kStart) {
			fprintf(stderr,"can't start: lockfile is already locked by another process\n");
			return LockStatus::kFail;
		}
		if (runmode==RunMode::kReload) {
			/*
			 * FIXME: buissiness logic should not be in file locking function.
			 */
			if (kill(ownerpid,SIGHUP)<0) {
				mfs_errlog(LOG_WARNING,"can't send reload signal to lock owner");
				return LockStatus::kFail;
			}
			fprintf(stderr,"reload signal has beed sent\n");
			return LockStatus::kSuccess;
		}
		if (runmode==RunMode::kKill) {
			fprintf(stderr,"sending SIGKILL to lock owner (pid:%ld)\n",(long int)ownerpid);
			/*
			 * FIXME: buissiness logic should not be in file locking function.
			 */
#ifdef ENABLE_EXIT_ON_USR1
			if (kill(ownerpid,SIGUSR1)<0) {
#else
			if (kill(ownerpid,SIGKILL)<0) {
#endif
				mfs_errlog(LOG_WARNING,"can't kill lock owner");
				return LockStatus::kFail;
			}
		} else {
			sassert((runmode == RunMode::kStop) || (runmode == RunMode::kRestart));
			fprintf(stderr,"sending SIGTERM to lock owner (pid:%ld)\n",(long int)ownerpid);
			/*
			 * FIXME: buissiness logic should not be in file locking function.
			 */
			if (kill(ownerpid,SIGTERM)<0) {
				mfs_errlog(LOG_WARNING,"can't kill lock owner");
				return LockStatus::kFail;
			}
		}
		fprintf(stderr,"waiting for termination ... ");
		fflush(stderr);
		uint32_t l = 0;
		pid_t newownerpid;
		do {
			newownerpid = mylock();
			if (newownerpid<0) {
				mfs_errlog(LOG_ERR,"fcntl error");
				return LockStatus::kFail;
			}
			if (newownerpid>0) {
				l++;
				if (l>=timeout) {
					syslog(LOG_ERR,"about %" PRIu32 " seconds passed and lockfile is still locked - giving up",l);
					fprintf(stderr,"giving up\n");
					return LockStatus::kFail;
				}
				if (l%10==0) {
					syslog(LOG_WARNING,"about %" PRIu32 " seconds passed and lock still exists",l);
					fprintf(stderr,"%" PRIu32 "s ",l);
					fflush(stderr);
				}
				if (newownerpid!=ownerpid) {
					fprintf(stderr,"\nnew lock owner detected\n");
					if (runmode==RunMode::kKill) {
						fprintf(stderr,"sending SIGKILL to lock owner (pid:%ld) ... ",(long int)newownerpid);
						fflush(stderr);
						if (kill(newownerpid,SIGKILL)<0) {
							mfs_errlog(LOG_WARNING,"can't kill lock owner");
							return LockStatus::kFail;
						}
					} else {
						sassert(runmode == RunMode::kRestart);
						fprintf(stderr,"sending SIGTERM to lock owner (pid:%ld) ... ",(long int)newownerpid);
						fflush(stderr);
						if (kill(newownerpid,SIGTERM)<0) {
							mfs_errlog(LOG_WARNING,"can't kill lock owner");
							return LockStatus::kFail;
						}
					}
					ownerpid = newownerpid;
				}
			}
			sleep(1);
		} while (newownerpid!=0);
		fprintf(stderr,"terminated\n");
		return (runmode == RunMode::kRestart) ? LockStatus::kAgain : LockStatus::kSuccess;
	}
	if (runmode==RunMode::kStart || runmode==RunMode::kRestart) {
		fprintf(stderr,"lockfile created and locked\n");
	} else if (runmode==RunMode::kStop || runmode==RunMode::kKill) {
		fprintf(stderr,"can't find process to terminate\n");
		return LockStatus::kFail;
	} else if (runmode==RunMode::kReload) {
		fprintf(stderr,"can't find process to send reload signal\n");
		return LockStatus::kFail;
	} else if (runmode==RunMode::kTest) {
		fprintf(stderr,STR(APPNAME) " is not running\n");
	} else if (runmode==RunMode::kIsAlive) {
		return LockStatus::kSuccess;
	}
	return LockStatus::kSuccess;
}

void makedaemon() {
	int f;
	uint8_t pipebuff[1000];
	ssize_t r;
	size_t happy;
	int piped[2];

	fflush(stdout);
	fflush(stderr);
	if (pipe(piped)<0) {
		fprintf(stderr,"pipe error\n");
		exit(LIZARDFS_EXIT_STATUS_ERROR);
	}
	f = fork();
	if (f<0) {
		syslog(LOG_ERR,"first fork error: %s",strerr(errno));
		exit(LIZARDFS_EXIT_STATUS_ERROR);
	}
	if (f>0) {
		wait(&f);       // just get child status - prevents child from being zombie during initialization stage
		if (f) {
			fprintf(stderr,"Child status: %d\n",f);
			exit(LIZARDFS_EXIT_STATUS_ERROR);
		}
		close(piped[1]);
		while ((r=read(piped[0],pipebuff,1000))) {
			if (r>0) {
				if (pipebuff[r-1]==0) { // zero as a last char in the pipe means error
					if (r>1) {
						happy = fwrite(pipebuff,1,r-1,stderr);
						(void)happy;
					}
					exit(LIZARDFS_EXIT_STATUS_ERROR);
				}
				happy = fwrite(pipebuff,1,r,stderr);
				(void)happy;
			} else {
				fprintf(stderr,"Error reading pipe: %s\n",strerr(errno));
				exit(LIZARDFS_EXIT_STATUS_ERROR);
			}
		}
		exit(LIZARDFS_EXIT_STATUS_SUCCESS);
	}
	setsid();
	setpgid(0,getpid());
	f = fork();
	if (f<0) {
		syslog(LOG_ERR,"second fork error: %s",strerr(errno));
		if (write(piped[1],"fork error\n",11)!=11) {
			syslog(LOG_ERR,"pipe write error: %s",strerr(errno));
		}
		close(piped[1]);
		exit(LIZARDFS_EXIT_STATUS_ERROR);
	}
	if (f>0) {
		exit(LIZARDFS_EXIT_STATUS_SUCCESS);
	}
	set_signal_handlers(1);

	close(STDIN_FILENO);
	sassert(open("/dev/null", O_RDWR, 0)==STDIN_FILENO);
	close(STDOUT_FILENO);
	sassert(dup(STDIN_FILENO)==STDOUT_FILENO);
	close(STDERR_FILENO);
	sassert(dup(piped[1])==STDERR_FILENO);
	close(piped[1]);
}

void close_msg_channel() {
	fflush(stderr);
	close(STDERR_FILENO);
	sassert(open("/dev/null", O_RDWR, 0)==STDERR_FILENO);
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
					mfs_arg_errlog(LOG_NOTICE,"creating directory %s",pathbuff);
				}
			} else {
				mfs_arg_syslog(LOG_NOTICE,"directory %s has been created",pathbuff);
			}
			*dst++=*src++;
		}
	}
}

void usage(const char *appname) {
	printf(
"usage: %s [-vdu] [-t locktimeout] [-c cfgfile] [start|stop|restart|reload|test|isalive]\n"
"\n"
"-v : print version number and exit\n"
"-d : run in foreground\n"
"-u : log undefined config variables\n"
"-t locktimeout : how long wait for lockfile\n"
"-c cfgfile : use given config file\n"
	,appname);
	exit(LIZARDFS_EXIT_STATUS_ERROR);
}

int main(int argc,char **argv) {
	char *wrkdir;
	char *cfgfile;
	char *appname;
	int ch;
	int logundefined;
	int lockmemory;
	int32_t nicelevel;
	uint32_t locktimeout;
	int fd;
	uint8_t movewarning;
	struct rlimit rls;

	prepareEnvironment();
	strerr_init();
	mycrc32_init();

	movewarning = 0;
	cfgfile=strdup(ETC_PATH "/mfs/" STR(APPNAME) ".cfg");
	passert(cfgfile);
	if ((fd = open(cfgfile,O_RDONLY))<0 && errno==ENOENT) {
		free(cfgfile);
		cfgfile=strdup(ETC_PATH "/" STR(APPNAME) ".cfg");
		passert(cfgfile);
		if ((fd = open(cfgfile,O_RDONLY))>=0) {
			movewarning = 1;
		}
	}
	if (fd>=0) {
		close(fd);
	}
	locktimeout = 1800;
	RunMode runmode = RunMode::kRestart;
	logundefined = 0;
	lockmemory = 0;
	appname = argv[0];

	while ((ch = getopt(argc, argv, "c:dht:uvx?")) != -1) {
		switch(ch) {
			case 'v':
				printf("version: %s\n",LIZARDFS_PACKAGE_VERSION);
				return 0;
			case 'd':
				gRunAsDaemon = false;
				break;
			case 't':
				locktimeout=strtoul(optarg,NULL,10);
				break;
			case 'c':
				free(cfgfile);
				cfgfile = strdup(optarg);
				passert(cfgfile);
				movewarning = 0;
				break;
			case 'u':
				logundefined=1;
				break;
			case 'x':
				++gVerbosity;
				break;
			default:
				usage(appname);
				return LIZARDFS_EXIT_STATUS_ERROR;
		}
	}
	argc -= optind;
	argv += optind;
	if (argc==1) {
		if (strcasecmp(argv[0],"start")==0) {
			runmode = RunMode::kStart;
		} else if (strcasecmp(argv[0],"stop")==0) {
			runmode = RunMode::kStop;
		} else if (strcasecmp(argv[0],"restart")==0) {
			runmode = RunMode::kRestart;
		} else if (strcasecmp(argv[0],"reload")==0) {
			runmode = RunMode::kReload;
		} else if (strcasecmp(argv[0],"test")==0) {
			runmode = RunMode::kTest;
		} else if (strcasecmp(argv[0],"kill")==0) {
			runmode = RunMode::kKill;
		} else if (strcasecmp(argv[0],"isalive")==0) {
			runmode = RunMode::kIsAlive;
		} else {
			usage(appname);
			return LIZARDFS_EXIT_STATUS_ERROR;
		}
	} else if (argc!=0) {
		usage(appname);
		return LIZARDFS_EXIT_STATUS_ERROR;
	}

	if (movewarning) {
		mfs_syslog(LOG_WARNING,"default sysconf path has changed - please move " STR(APPNAME) ".cfg from " ETC_PATH "/ to " ETC_PATH "/mfs/");
	}

	if (runmode==RunMode::kStart || runmode==RunMode::kRestart) {
		if (gRunAsDaemon) {
			makedaemon();
		} else {
			set_signal_handlers(0);
		}
	}

	if (cfg_load(cfgfile,logundefined)==0) {
		fprintf(stderr,"can't load config file: %s - using defaults\n",cfgfile);
	}
	free(cfgfile);

	const std::string& logappname = set_syslog_ident();

	if (runmode==RunMode::kStart || runmode==RunMode::kRestart) {
		rls.rlim_cur = MFSMAXFILES;
		rls.rlim_max = MFSMAXFILES;
		if (setrlimit(RLIMIT_NOFILE,&rls)<0) {
			syslog(LOG_NOTICE,"can't change open files limit to %u",MFSMAXFILES);
		}

		lockmemory = cfg_getnum("LOCK_MEMORY",0);
#ifdef MFS_USE_MEMLOCK
		if (lockmemory) {
			rls.rlim_cur = RLIM_INFINITY;
			rls.rlim_max = RLIM_INFINITY;
			setrlimit(RLIMIT_MEMLOCK,&rls);
		}
#endif
		nicelevel = cfg_getint32("NICE_LEVEL",-19);
		setpriority(PRIO_PROCESS,getpid(),nicelevel);
	}

	changeugid();

	wrkdir = cfg_getstr("DATA_PATH",DATA_PATH);
	if (runmode==RunMode::kStart || runmode==RunMode::kRestart) {
		fprintf(stderr,"working directory: %s\n",wrkdir);
	}

	if (chdir(wrkdir)<0) {
		mfs_arg_syslog(LOG_ERR,"can't set working directory to %s",wrkdir);
		if (gRunAsDaemon) {
			fputc(0,stderr);
			close_msg_channel();
		}
		closelog();
		return LIZARDFS_EXIT_STATUS_ERROR;
	}
	free(wrkdir);

	umask(cfg_getuint32("FILE_UMASK",027)&077);

	std::unique_ptr<FileLock> fl;
	try {
		/*
		 * Only kStart should check for lock file consistency.
		 */
		fl.reset(new FileLock(runmode, locktimeout));
	} catch (const FilesystemException& e) {
		if (e.what()[0]) {
			mfs_errlog(LOG_ERR, e.what());
		}
		if (gRunAsDaemon) {
			fputc(0,stderr);
			close_msg_channel();
		}
		closelog();
		return LIZARDFS_EXIT_STATUS_ERROR;
	}

	if (runmode==RunMode::kStop || runmode==RunMode::kKill || runmode==RunMode::kReload
			|| runmode==RunMode::kTest || runmode==RunMode::kIsAlive) {
		if (gRunAsDaemon) {
			close_msg_channel();
		}
		closelog();
		if (runmode==RunMode::kIsAlive) {
			FileLock::LockStatus lockstatus = fl->lockstatus();
			sassert((lockstatus == FileLock::LockStatus::kSuccess)
					|| (lockstatus == FileLock::LockStatus::kAlive));
			return (lockstatus == FileLock::LockStatus::kAlive
					? LIZARDFS_EXIT_STATUS_SUCCESS : LIZARDFS_EXIT_STATUS_NOT_ALIVE);
		} else {
			return LIZARDFS_EXIT_STATUS_SUCCESS;
		}
	}

#ifdef MFS_USE_MEMLOCK
	if (lockmemory) {
		if (getrlimit(RLIMIT_MEMLOCK,&rls)<0) {
			mfs_errlog(LOG_WARNING,"error getting memory lock limits");
		} else {
			if (rls.rlim_cur!=RLIM_INFINITY && rls.rlim_max==RLIM_INFINITY) {
				rls.rlim_cur = RLIM_INFINITY;
				rls.rlim_max = RLIM_INFINITY;
				if (setrlimit(RLIMIT_MEMLOCK,&rls)<0) {
					mfs_errlog(LOG_WARNING,"error setting memory lock limit to unlimited");
				}
			}
			if (getrlimit(RLIMIT_MEMLOCK,&rls)<0) {
				mfs_errlog(LOG_WARNING,"error getting memory lock limits");
			} else {
				if (rls.rlim_cur!=RLIM_INFINITY) {
					mfs_errlog(LOG_WARNING,"can't set memory lock limit to unlimited");
				} else {
					if (mlockall(MCL_CURRENT|MCL_FUTURE)<0) {
						mfs_errlog(LOG_WARNING,"memory lock error");
					} else {
						mfs_syslog(LOG_NOTICE,"process memory was successfully locked in RAM");
					}
			}       }
		}
	}
#else
	if (lockmemory) {
		mfs_syslog(LOG_WARNING,"memory lock not supported !!!");
	}
#endif
	fprintf(stderr, "initializing %s modules ...\n", logappname.c_str());

	if (initialize()) {
		if (getrlimit(RLIMIT_NOFILE,&rls)==0) {
			syslog(LOG_NOTICE,"open files limit: %lu",(unsigned long)(rls.rlim_cur));
		}
		fprintf(stderr, "%s daemon initialized properly\n", logappname.c_str());
		if (gRunAsDaemon) {
			close_msg_channel();
		}
		if (initialize_late()) {
			main_reloadregister(main_reload); // this will be the first thing to do
			mainloop();
			ch=LIZARDFS_EXIT_STATUS_SUCCESS;
		} else {
			ch=LIZARDFS_EXIT_STATUS_ERROR;
		}
	} else {
		fprintf(stderr,"error occured during initialization - exiting\n");
		if (gRunAsDaemon) {
			fputc(0,stderr);
			close_msg_channel();
		}
		ch=LIZARDFS_EXIT_STATUS_ERROR;
	}
	destruct();
	free_all_registered_entries();
	signal_cleanup();
	cfg_term();
	strerr_term();
	closelog();
	return ch;
}
