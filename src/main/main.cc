/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o..

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

#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <pwd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
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
#include "common/time_utils.h"
#include "protocol/MFSCommunication.h"

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

#if defined(LIZARDFS_HAVE_PAM)
#  include <security/pam_appl.h>
#  include <security/pam_misc.h>
#endif

#ifndef LIZARDFS_MAX_FILES
#  define LIZARDFS_MAX_FILES 5000
#endif

#if defined(LIZARDFS_HAVE_SYSTEMD_SD_DAEMON_H)
#include <systemd/sd-daemon.h>
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

enum class ExitingStatus {
	kRunning = 0,
	kWantExit = 1,
	kCanExit = 2,
	kDoExit = 3
};

static bool nextPollNonblocking = false;

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
	void (*desc)(std::vector<pollfd>&);
	void (*serve)(const std::vector<pollfd>&);
} pollentry;

namespace {
std::list<pollentry> gPollEntries;
}

typedef struct eloopentry {
	void (*fun)(void);
	struct eloopentry *next;
} eloopentry;

static eloopentry *eloophead=NULL;

#if defined(LIZARDFS_HAVE_PAM)
static pam_handle_t *gPAMHandle=NULL;
#endif

struct timeentry {
	typedef void (*fun_t)(void);
	timeentry(uint64_t ne, uint64_t sec, uint64_t off, int mod, fun_t f, bool ms)
		: nextevent(ne), period(sec), offset(off), mode(mod), fun(f), millisecond_precision(ms) {
	}
	uint64_t nextevent;
	uint64_t period;
	uint64_t offset;
	int      mode;
	fun_t    fun;
	bool     millisecond_precision;
};

typedef std::list<timeentry> TimeEntries;
namespace {
TimeEntries gTimeEntries;
}

static std::atomic<uint32_t> now;
static std::atomic<uint64_t> usecnow;
static bool gRunAsDaemon = true;
static std::vector<std::string> gExtraArguments;
static ExitingStatus gExitingStatus = ExitingStatus::kRunning;

/// When set to true, config will be reloaded after the current loop
static bool gReloadRequested = false;

static int signalpipe[2];

/* interface */

const std::vector<std::string>& main_get_extra_arguments() {
	return gExtraArguments;
}

bool main_has_extra_argument(std::string name, CaseSensitivity mode) {
	if (mode == CaseSensitivity::kSensitive) {
		std::transform(name.begin(), name.end(), name.begin(), tolower);
	}
	for (auto option : gExtraArguments) {
		if (mode == CaseSensitivity::kSensitive) {
			std::transform(option.begin(), option.end(), option.begin(), tolower);
		}
		if (option == name) {
			return true;
		}
	}
	return false;
}

void main_make_next_poll_nonblocking() {
	nextPollNonblocking = true;
}

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

void main_pollregister (void (*desc)(std::vector<pollfd>&),void (*serve)(const std::vector<pollfd>&)) {
	gPollEntries.push_back({desc,serve});
}

void main_eachloopregister (void (*fun)(void)) {
	eloopentry *aux=(eloopentry*)malloc(sizeof(eloopentry));
	passert(aux);
	aux->fun = fun;
	aux->next = eloophead;
	eloophead = aux;
}

void *main_timeregister(int mode, uint64_t seconds, uint64_t offset, void (*fun)(void)) {
	if (seconds == 0 || offset >= seconds) {
		return NULL;
	}

	uint64_t nextevent = ((now + seconds) / seconds) * seconds + offset;

	gTimeEntries.push_front(timeentry(nextevent, seconds, offset, mode, fun, false));
	return &gTimeEntries.front();
}

void *main_timeregister_ms(uint64_t period, void (*fun)(void)) {
	if (period == 0) {
		return NULL;
	}

	uint64_t nextevent = usecnow / 1000 + period;

	gTimeEntries.push_front(timeentry(nextevent, period, 0, TIMEMODE_RUN_LATE, fun, true));
	return &gTimeEntries.front();
}

void main_timeunregister(void* handler) {
	for (TimeEntries::iterator it = gTimeEntries.begin(); it != gTimeEntries.end(); ++it) {
		if (&(*it) == handler) {
			gTimeEntries.erase(it);
			return;
		}
	}
	mabort("unregistering unknown handle from time table");
}

int main_timechange(void* handle, int mode, uint64_t seconds, uint64_t offset) {
	timeentry *aux = (timeentry*)handle;
	if (seconds == 0 || offset >= seconds) {
		return -1;
	}
	aux->nextevent = ((now + seconds) / seconds) * seconds + offset;
	aux->period = seconds;
	aux->offset = offset;
	aux->mode = mode;
	return 0;
}

int main_timechange_ms(void* handle, uint64_t period) {
	timeentry *aux = (timeentry*)handle;
	if (period == 0) {
		return -1;
	}
	aux->nextevent = ((usecnow / 1000 + period) / period) * period;
	aux->period = period;
	return 0;
}

/* internal */

void free_all_registered_entries(void) {
	deentry *de,*den;
	ceentry *ce,*cen;
	weentry *we,*wen;
	rlentry *re,*ren;
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

	gPollEntries.clear();

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

uint8_t main_want_to_terminate() {
	if (gExitingStatus == ExitingStatus::kRunning) {
		gExitingStatus = ExitingStatus::kWantExit;
		syslog(LOG_INFO, "Exiting on internal request.");
		return LIZARDFS_STATUS_OK;
	} else {
		syslog(LOG_ERR, "Unable to exit on internal request.");
		return LIZARDFS_ERROR_NOTPOSSIBLE;
	}
}

void main_want_to_reload() {
	gReloadRequested = true;
}

void destruct() {
	deentry *deit;
	for (deit = dehead ; deit!=NULL ; deit=deit->next) {
		try {
			deit->fun();
		} catch (Exception& ex) {
			syslog(LOG_WARNING, "term error: %s", ex.what());
		}
	}
}

void mainloop() {
	uint32_t prevtime  = 0;
	uint64_t prevmtime = 0;
	struct timeval tv;
	eloopentry *eloopit;
	ceentry *ceit;
	weentry *weit;
	rlentry *rlit;
	std::vector<pollfd> pdesc;
	int i;

	while (gExitingStatus != ExitingStatus::kDoExit) {
		pdesc.clear();
		pdesc.push_back({signalpipe[0],POLLIN,0});
		for (auto &pollit: gPollEntries) {
			pollit.desc(pdesc);
		}
		i = poll(pdesc.data(),pdesc.size(), nextPollNonblocking ? 0 : 50);
		nextPollNonblocking = false;
		gettimeofday(&tv,NULL);
		usecnow = tv.tv_sec * uint64_t(1000000) + tv.tv_usec;
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
					if (sigid == '\001' && gExitingStatus == ExitingStatus::kRunning) {
						syslog(LOG_NOTICE,"terminate signal received");
						gExitingStatus = ExitingStatus::kWantExit;
					} else if (sigid=='\002') {
						syslog(LOG_NOTICE,"reloading config files");
						gReloadRequested = true;
					} else if (sigid=='\003') {
						syslog(LOG_NOTICE, "Received SIGUSR1, killing gently...");
						exit(LIZARDFS_EXIT_STATUS_GENTLY_KILL);
					}
				}
			}
			for (auto &pollit : gPollEntries) {
				pollit.serve(pdesc);
			}
		}
		for (eloopit = eloophead ; eloopit != NULL ; eloopit = eloopit->next) {
			eloopit->fun();
		}

		uint64_t msecnow = usecnow / 1000;

		if (msecnow < prevmtime) {
			// time went backward - recalculate next event time
			for (timeentry& timeit : gTimeEntries) {
				if (!timeit.millisecond_precision) {
					continue;
				}

				uint64_t previous_time_to_run = timeit.nextevent - prevmtime;
				previous_time_to_run = std::min(previous_time_to_run, timeit.period);
				timeit.nextevent = msecnow + previous_time_to_run;
			}
		}

		if (now<prevtime) {
			// time went backward !!! - recalculate "nextevent" time
			// adding previous_time_to_run prevents from running next event too soon.
			for (timeentry& timeit : gTimeEntries) {
				if (timeit.millisecond_precision) {
					continue;
				}

				uint64_t previous_time_to_run = timeit.nextevent - prevtime;
				previous_time_to_run = std::min(previous_time_to_run, timeit.period);
				timeit.nextevent = ((now + previous_time_to_run + timeit.period)
				                   / timeit.period) * timeit.period + timeit.offset;
			}
		} else if (now>prevtime+3600) {
			// time went forward !!! - just recalculate "nextevent" time
			for (timeentry& timeit : gTimeEntries) {
				if (timeit.millisecond_precision) {
					timeit.nextevent = msecnow + timeit.period;
					continue;
				}

				timeit.nextevent = ((now + timeit.period) / timeit.period)
				                    * timeit.period + timeit.offset;
			}
		}

		for (timeentry& timeit : gTimeEntries) {
			if (timeit.millisecond_precision) {
				if (msecnow >= timeit.nextevent) {
					timeit.nextevent = msecnow + timeit.period;
					timeit.fun();
				}
				continue;
			}

			if (now >= timeit.nextevent) {
				if (timeit.mode == TIMEMODE_RUN_LATE) {
					timeit.fun();
				} else { /* timeit.mode == TIMEMODE_SKIP_LATE */
					if (now == timeit.nextevent) {
						timeit.fun();
					}
				}
				timeit.nextevent += ((now - timeit.nextevent + timeit.period)
				                    / timeit.period) * timeit.period;
			}
		}
		prevtime  = now;
		prevmtime = usecnow / 1000;
		if (gExitingStatus == ExitingStatus::kRunning && gReloadRequested) {
			cfg_reload();
			for (rlit = rlhead ; rlit!=NULL ; rlit=rlit->next) {
				try {
					rlit->fun();
				} catch (Exception& ex) {
					syslog(LOG_WARNING, "reload error: %s", ex.what());
				}
			}
			gReloadRequested = false;
			DEBUG_LOG("main.reload");
		}
		if (gExitingStatus == ExitingStatus::kWantExit) {
			for (weit = wehead ; weit!=NULL ; weit=weit->next) {
				weit->fun();
			}
			gExitingStatus = ExitingStatus::kCanExit;
		}
		if (gExitingStatus == ExitingStatus::kCanExit) {
			i = 1;
			for (ceit = cehead ; ceit!=NULL && i ; ceit=ceit->next) {
				if (ceit->fun()==0) {
					i=0;
				}
			}
			if (i) {
				gExitingStatus = ExitingStatus::kDoExit;
			}
		}
	}
}

int initialize(run_tab* tab) {
	uint32_t i;
	int ok;
	ok = 1;
	for (i=0 ; (long int)(tab[i].fn)!=0 && ok ; i++) {
		now = time(NULL);
		try {
			if (tab[i].fn()<0) {
				lzfs_pretty_syslog(LOG_ERR,"init: %s failed",tab[i].name);
				ok=0;
			}
		} catch (const std::exception& e) {
			lzfs_pretty_syslog(LOG_ERR, "%s", e.what());
			ok = 0;
		}
	}
	now = time(NULL);
	return ok;
}

int initialize_early(void) {
	return initialize(EarlyRunTab);
}

int initialize(void) {
	return initialize(RunTab);
}

int initialize_late(void) {
	return initialize(LateRunTab);
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
		lzfs_disable_printf();
	} else {
#if defined(LOG_PERROR)
		openlog(logIdent.c_str(), LOG_PID | LOG_NDELAY | LOG_PERROR, LOG_USER);
		lzfs_disable_printf();
#else
		openlog(logIdent.c_str(), LOG_PID | LOG_NDELAY, LOG_USER);
#endif
	}
	return logIdent;
}

static void main_configure_debug_log() {
	std::string debugLogConfiguration;
	for (std::string suffix : {"", "_A", "_B", "_C"}) {
		std::string configEntryName = "MAGIC_DEBUG_LOG" + suffix;
		std::string value = cfg_get(configEntryName.c_str(), "");
		if (value.empty()) {
			continue;
		}
		if (!debugLogConfiguration.empty()) {
			debugLogConfiguration += ',';
		}
		debugLogConfiguration += value;
	}
	DebugLog::setConfiguration(std::move(debugLogConfiguration));
}

void main_reload() {
	// Reload SYSLOG_IDENT
	syslog(LOG_NOTICE, "Changing SYSLOG_IDENT to %s",
			cfg_get("SYSLOG_IDENT", STR(APPNAME)).c_str());
	set_syslog_ident();

	// Reload MAGIC_DEBUG_LOG
	main_configure_debug_log();
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

void changeugid(RunMode runmode) {
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
				lzfs_pretty_syslog(LOG_WARNING,"%s: no such group",wgroup);
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
					lzfs_pretty_syslog(LOG_ERR,"%s: no such user id - can't obtain group id",wuser+1);
					exit(LIZARDFS_EXIT_STATUS_ERROR);
				}
				wrk_gid = pw->pw_gid;
			}
		} else {
			getpwnam_r(wuser,&pwd,pwdgrpbuff,16384,&pw);
			if (pw==NULL) {
				lzfs_pretty_syslog(LOG_ERR,"%s: no such user",wuser);
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
			lzfs_pretty_errlog(LOG_ERR,"can't set gid to %d",(int)wrk_gid);
			exit(LIZARDFS_EXIT_STATUS_ERROR);
		} else if ((runmode == RunMode::kStart) || (runmode == RunMode::kRestart)){
			syslog(LOG_NOTICE,"set gid to %d",(int)wrk_gid);
		}
		if (setuid(wrk_uid)<0) {
			lzfs_pretty_errlog(LOG_ERR,"can't set uid to %d",(int)wrk_uid);
			exit(LIZARDFS_EXIT_STATUS_ERROR);
		} else if ((runmode == RunMode::kStart) || (runmode == RunMode::kRestart)){
			syslog(LOG_NOTICE,"set uid to %d",(int)wrk_uid);
		}
	}
}

bool open_pam_session() {
#if defined(LIZARDFS_HAVE_PAM)
	static struct pam_conv conv = {misc_conv, NULL};

	assert(gPAMHandle == NULL);

	int retval;

	retval = pam_start("lizardfs", DEFAULT_USER, &conv, &gPAMHandle);
	if (retval != PAM_SUCCESS) {
		lzfs_pretty_errlog(LOG_ERR, "Can't initialize PAM");
		return false;
	}

	retval = pam_open_session(gPAMHandle, 0);
	if (retval != PAM_SUCCESS) {
		lzfs_pretty_errlog(LOG_ERR, "Can't open PAM session");
		return false;
	}

	return true;
#else
	return false;
#endif
}

void close_pam_session() {
#if defined(LIZARDFS_HAVE_PAM)
	if (!gPAMHandle) {
		return;
	}

	int retval = pam_close_session(gPAMHandle, 0);
	pam_end(gPAMHandle, retval);
#endif
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
	}
	~FileLock() {
		/*
		 * Order of unlock/file deletion matters
		 * for RunMode::kRestart mode.
		 * We need to remove file first and then
		 * unlock file.
		 */
		if (lockstatus_ != LockStatus::kFail && thisProcessCreatedLockFile_) {
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
	bool createLockFile();

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

bool FileLock::createLockFile() {
	bool notExisted((::access(name_.c_str(), F_OK) != 0) && (errno == ENOENT));
	fd_.reset(open(name_.c_str(), O_WRONLY | O_CREAT, 0666));
	if (!fd_.isOpened()) {
		std::string err = "can't create lockfile " + fs::getCurrentWorkingDirectoryNoThrow()
		                  + "/" + name();
		lzfs_pretty_errlog(LOG_ERR, "%s", err.c_str());

		return false;
	}
	thisProcessCreatedLockFile_ = notExisted;
	return true;
}

FileLock::LockStatus FileLock::wdlock(RunMode runmode, uint32_t timeout) {
	std::string lockPath = fs::getCurrentWorkingDirectoryNoThrow() + "/" + name_;
	if(!createLockFile()) {
		return LockStatus::kFail;
	}

	pid_t ownerpid(mylock());
	if (ownerpid<0) {
		lzfs_pretty_errlog(LOG_ERR, "fcntl error while creating lockfile %s", lockPath.c_str());
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
			lzfs_pretty_syslog(LOG_ERR,
					"can't start: lockfile %s is already locked by another process",
					lockPath.c_str());
			return LockStatus::kFail;
		}
		if (runmode==RunMode::kReload) {
			/*
			 * FIXME: buissiness logic should not be in file locking function.
			 */
			if (kill(ownerpid,SIGHUP)<0) {
				lzfs_pretty_errlog(LOG_ERR,
						"can't send reload signal to the lock owner of %s",
						lockPath.c_str());
				return LockStatus::kFail;
			}
			lzfs_pretty_syslog(LOG_INFO, "reload signal has been sent");
			return LockStatus::kSuccess;
		}
		if (runmode==RunMode::kKill) {
			/*
			 * FIXME: buissiness logic should not be in file locking function.
			 */
#ifdef ENABLE_EXIT_ON_USR1
			if (kill(ownerpid,SIGUSR1)<0) {
#else
			if (kill(ownerpid,SIGKILL)<0) {
#endif
				lzfs_pretty_errlog(LOG_ERR,
						"can't kill (SIGKILL) the lock owner (pid:%ld) of %s",
						(long int)ownerpid, lockPath.c_str());
				return LockStatus::kFail;
			}
		} else {
			sassert((runmode == RunMode::kStop) || (runmode == RunMode::kRestart));
			/*
			 * FIXME: buissiness logic should not be in file locking function.
			 */
			if (kill(ownerpid,SIGTERM)<0) {
				lzfs_pretty_errlog(LOG_ERR,
						"can't kill (SIGTERM) the lock owner (pid:%ld) of %s",
						(long int)ownerpid, lockPath.c_str());
				return LockStatus::kFail;
			}
		}
		fflush(stderr);
		uint32_t l = 0;
		pid_t newownerpid;
		uint32_t checksPerSecond = 10;
		do {
			newownerpid = mylock();
			if (newownerpid<0) {
				lzfs_pretty_errlog(LOG_ERR,
						"fcntl error while creating lockfile %s",
						lockPath.c_str());
				return LockStatus::kFail;
			}
			if (newownerpid>0) {
				l++;
				uint32_t secondsElapsed = l / checksPerSecond;
				if (secondsElapsed >= timeout) {
					lzfs_pretty_syslog(LOG_ERR,
							"about %" PRIu32 " seconds passed and the lock %s still exists - giving up",
							secondsElapsed, lockPath.c_str());
					return LockStatus::kFail;
				}
				if (l % (10 * checksPerSecond) == 0) {
					lzfs_pretty_syslog(LOG_WARNING,
							"about %" PRIu32 " seconds passed and the lock %s still exists...",
							secondsElapsed, lockPath.c_str());
					fflush(stderr);
				}
				if (newownerpid!=ownerpid) {
					lzfs_silent_syslog(LOG_INFO, "new lock owner of %s detected", lockPath.c_str());
					if (runmode==RunMode::kKill) {
						if (kill(newownerpid,SIGKILL)<0) {
							lzfs_pretty_errlog(LOG_ERR,
									"can't kill (SIGKILL) the lock owner (pid:%ld) of %s",
									(long int)newownerpid, lockPath.c_str());
							return LockStatus::kFail;
						}
					} else {
						sassert((runmode == RunMode::kStop) || (runmode == RunMode::kRestart));
						if (kill(newownerpid,SIGTERM)<0) {
							lzfs_pretty_errlog(LOG_ERR,
									"can't kill (SIGTERM) lock owner (pid:%ld) of %s",
									(long int)newownerpid, lockPath.c_str());
							return LockStatus::kFail;
						}
					}
					ownerpid = newownerpid;
				}
			}
			usleep(1000000 / checksPerSecond);
		} while (newownerpid!=0);
		return (runmode == RunMode::kRestart) ? LockStatus::kAgain : LockStatus::kSuccess;
	}
	if (runmode==RunMode::kStart || runmode==RunMode::kRestart) {
		lzfs_pretty_syslog(LOG_INFO,"lockfile %s created and locked", lockPath.c_str());
	} else if (runmode==RunMode::kStop || runmode==RunMode::kKill) {
		lzfs_pretty_syslog(LOG_WARNING,"can't find process to terminate");
		return LockStatus::kSuccess;
	} else if (runmode==RunMode::kReload) {
		lzfs_pretty_syslog(LOG_ERR,"can't find process to send reload signal");
		return LockStatus::kFail;
	} else if (runmode==RunMode::kTest) {
		fprintf(stderr, STR(APPNAME) " is not running\n");
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
		lzfs_pretty_syslog(LOG_ERR, "pipe error");
		exit(LIZARDFS_EXIT_STATUS_ERROR);
	}
	f = fork();
	if (f<0) {
		lzfs_pretty_errlog(LOG_ERR, "first fork error");
		exit(LIZARDFS_EXIT_STATUS_ERROR);
	}
	if (f>0) {
		wait(&f);       // just get child status - prevents child from being zombie during initialization stage
		if (f) {
			lzfs_pretty_syslog(LOG_ERR, "child status: %d",f);
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
				lzfs_pretty_errlog(LOG_ERR,"error reading pipe");
				exit(LIZARDFS_EXIT_STATUS_ERROR);
			}
		}
		exit(LIZARDFS_EXIT_STATUS_SUCCESS);
	}
	setsid();
	setpgid(0,getpid());
	f = fork();
	if (f<0) {
		lzfs_pretty_errlog(LOG_ERR,"second fork error");
		if (write(piped[1],"fork error\n",11)!=11) {
			lzfs_pretty_errlog(LOG_ERR,"pipe write error");
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

	// close all inherited file descriptors
	int open_max = sysconf(_SC_OPEN_MAX);
	for (int i = 3; i < open_max; i++) {
		if (i == signalpipe[0] || i == signalpipe[1]) {
			continue;
		}
		close(i);
	}
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
					lzfs_pretty_errlog(LOG_NOTICE,"creating directory %s",pathbuff);
				}
			} else {
				lzfs_pretty_syslog(LOG_NOTICE,"directory %s has been created",pathbuff);
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
"-o extra_option : module specific extra option\n"
	,appname);
	exit(LIZARDFS_EXIT_STATUS_ERROR);
}

int main(int argc,char **argv) {
	char *wrkdir;
	char *appname;
	int ch;
	int logundefined;
	int lockmemory;
	int32_t nicelevel;
	uint32_t locktimeout;
	struct rlimit rls;
	std::string default_cfgfile = ETC_PATH "/" STR(APPNAME) ".cfg";
	std::string cfgfile = default_cfgfile;

	prepareEnvironment();
	strerr_init();
	mycrc32_init();

	locktimeout = 1800;
	RunMode runmode = RunMode::kRestart;
	logundefined = 0;
	lockmemory = 0;
	appname = argv[0];

	while ((ch = getopt(argc, argv, "o:c:dht:uvx?")) != -1) {
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
			case 'o':
				gExtraArguments.emplace_back(optarg);
				break;
			case 'c':
				cfgfile = optarg;
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

	if (runmode==RunMode::kStart || runmode==RunMode::kRestart) {
		if (gRunAsDaemon) {
			makedaemon();
		} else {
			set_signal_handlers(0);
		}
	}

	if (cfg_load(cfgfile.c_str(), logundefined)==0) {
		lzfs_pretty_syslog(LOG_WARNING, "configuration file %s not found - using defaults; "
				"please create one to remove this warning "
				"(you can copy %s.dist to get a base configuration)",
				cfgfile.c_str(), default_cfgfile.c_str());
	} else if (runmode==RunMode::kStart || runmode==RunMode::kRestart) {
		lzfs_pretty_syslog(LOG_INFO, "configuration file %s loaded", cfgfile.c_str());
	}
	main_configure_debug_log();

	if (runmode==RunMode::kStart || runmode==RunMode::kRestart) {
		if (!open_pam_session()) {
			rls.rlim_cur = LIZARDFS_MAX_FILES;
			rls.rlim_max = LIZARDFS_MAX_FILES;
			if (setrlimit(RLIMIT_NOFILE, &rls) < 0) {
				lzfs_pretty_syslog(
				    LOG_WARNING,
				    "can't change open files limit to %u, check restrictions with `ulimit -n'",
				    LIZARDFS_MAX_FILES);
			} else {
				lzfs_pretty_syslog(
				    LOG_INFO, "failed to use system open files limit, changed to default value %u",
				    LIZARDFS_MAX_FILES);
			}
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

	changeugid(runmode);

	wrkdir = cfg_getstr("DATA_PATH",DATA_PATH);


	if (chdir(wrkdir)<0) {
		lzfs_pretty_syslog(LOG_ERR,"can't set working directory to %s",wrkdir);
		if (gRunAsDaemon) {
			fputc(0,stderr);
			close_msg_channel();
		}
		closelog();
		return LIZARDFS_EXIT_STATUS_ERROR;
	} else {
		if (runmode==RunMode::kStart || runmode==RunMode::kRestart) {
			lzfs_pretty_syslog(LOG_INFO,"changed working directory to: %s",wrkdir);
		}
	}
	free(wrkdir);

	umask(cfg_getuint32("FILE_UMASK",027)&077);

	if (!initialize_early()) {
		if (gRunAsDaemon) {
			fputc(0, stderr);
			close_msg_channel();
		}
		closelog();
		return LIZARDFS_EXIT_STATUS_ERROR;
	}

	// Only kStart should check for lock file consistency
	FileLock fl(runmode, locktimeout);
	if (fl.lockstatus() == FileLock::LockStatus::kFail) {
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
			FileLock::LockStatus lockstatus = fl.lockstatus();
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
			lzfs_pretty_errlog(LOG_WARNING,"error getting memory lock limits");
		} else {
			if (rls.rlim_cur!=RLIM_INFINITY && rls.rlim_max==RLIM_INFINITY) {
				rls.rlim_cur = RLIM_INFINITY;
				rls.rlim_max = RLIM_INFINITY;
				if (setrlimit(RLIMIT_MEMLOCK,&rls)<0) {
					lzfs_pretty_errlog(LOG_WARNING,"error setting memory lock limit to unlimited");
				}
			}
			if (getrlimit(RLIMIT_MEMLOCK,&rls)<0) {
				lzfs_pretty_errlog(LOG_WARNING,"error getting memory lock limits");
			} else {
				if (rls.rlim_cur!=RLIM_INFINITY) {
					lzfs_pretty_errlog(LOG_WARNING,"can't set memory lock limit to unlimited");
				} else {
					if (mlockall(MCL_CURRENT|MCL_FUTURE)<0) {
						lzfs_pretty_errlog(LOG_WARNING,"memory lock error");
					} else {
						lzfs_pretty_syslog(LOG_NOTICE,"process memory was successfully locked in RAM");
					}
				}
			}
		}
	}
#else
	if (lockmemory) {
		lzfs_pretty_syslog(LOG_WARNING,"trying to lock memory, but memory lock not supported");
	}
#endif

	if (initialize()) {
		if (getrlimit(RLIMIT_NOFILE,&rls)==0) {
			lzfs_pretty_syslog(LOG_NOTICE,"open files limit: %lu",(unsigned long)(rls.rlim_cur));
		}
		lzfs_pretty_syslog(LOG_NOTICE, STR(APPNAME)" daemon initialized properly");
#ifdef LIZARDFS_HAVE_SYSTEMD_SD_DAEMON_H
		sd_notify(0, "READY=1\nSTATUS=" STR(APPNAME) " daemon initialized properly.\n");
#endif
		set_syslog_ident();
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
	close_pam_session();
	return ch;
}
