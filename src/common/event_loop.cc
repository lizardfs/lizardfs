/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2017 Skytechnology sp. z o.o..

   This file is part of LizardFS.

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

#include "event_loop.h"

#include <atomic>
#include <list>
#include <sys/time.h>
#include <unistd.h>

#include "common/cfg.h"
#include "common/exception.h"
#include "common/massert.h"

ExitingStatus gExitingStatus = ExitingStatus::kRunning;
bool gReloadRequested = false;
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


void eventloop_make_next_poll_nonblocking() {
	nextPollNonblocking = true;
}

void eventloop_destructregister (void (*fun)(void)) {
	deentry *aux=(deentry*)malloc(sizeof(deentry));
	passert(aux);
	aux->fun = fun;
	aux->next = dehead;
	dehead = aux;
}

void eventloop_canexitregister (int (*fun)(void)) {
	ceentry *aux=(ceentry*)malloc(sizeof(ceentry));
	passert(aux);
	aux->fun = fun;
	aux->next = cehead;
	cehead = aux;
}

void eventloop_wantexitregister (void (*fun)(void)) {
	weentry *aux=(weentry*)malloc(sizeof(weentry));
	passert(aux);
	aux->fun = fun;
	aux->next = wehead;
	wehead = aux;
}

void eventloop_reloadregister (void (*fun)(void)) {
	rlentry *aux=(rlentry*)malloc(sizeof(rlentry));
	passert(aux);
	aux->fun = fun;
	aux->next = rlhead;
	rlhead = aux;
}

void eventloop_pollregister (void (*desc)(std::vector<pollfd>&),void (*serve)(const std::vector<pollfd>&)) {
	gPollEntries.push_back({desc,serve});
}

void eventloop_eachloopregister (void (*fun)(void)) {
	eloopentry *aux=(eloopentry*)malloc(sizeof(eloopentry));
	passert(aux);
	aux->fun = fun;
	aux->next = eloophead;
	eloophead = aux;
}

void *eventloop_timeregister(int mode, uint64_t seconds, uint64_t offset, void (*fun)(void)) {
	if (seconds == 0 || offset >= seconds) {
		return NULL;
	}

	uint64_t nextevent = ((now + seconds) / seconds) * seconds + offset;

	gTimeEntries.push_front(timeentry(nextevent, seconds, offset, mode, fun, false));
	return &gTimeEntries.front();
}

void *eventloop_timeregister_ms(uint64_t period, void (*fun)(void)) {
	if (period == 0) {
		return NULL;
	}

	uint64_t nextevent = usecnow / 1000 + period;

	gTimeEntries.push_front(timeentry(nextevent, period, 0, TIMEMODE_RUN_LATE, fun, true));
	return &gTimeEntries.front();
}

void eventloop_timeunregister(void* handler) {
	for (TimeEntries::iterator it = gTimeEntries.begin(); it != gTimeEntries.end(); ++it) {
		if (&(*it) == handler) {
			gTimeEntries.erase(it);
			return;
		}
	}
	mabort("unregistering unknown handle from time table");
}

int eventloop_timechange(void* handle, int mode, uint64_t seconds, uint64_t offset) {
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

int eventloop_timechange_ms(void* handle, uint64_t period) {
	timeentry *aux = (timeentry*)handle;
	if (period == 0) {
		return -1;
	}
	aux->nextevent = ((usecnow / 1000 + period) / period) * period;
	aux->period = period;
	return 0;
}

void eventloop_destruct() {
	deentry *deit;
	for (deit = dehead ; deit!=NULL ; deit=deit->next) {
		try {
			deit->fun();
		} catch (Exception& ex) {
			syslog(LOG_WARNING, "term error: %s", ex.what());
		}
	}
}

void eventloop_release_resources(void) {
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

/* internal */
int canexit() {
	ceentry *aux;
	for (aux = cehead ; aux!=NULL ; aux=aux->next) {
		if (aux->fun()==0) {
			return 0;
		}
	}
	return 1;
}

uint32_t eventloop_time() {
	return now;
}

uint64_t eventloop_utime() {
	return usecnow;
}

uint8_t eventloop_want_to_terminate() {
	if (gExitingStatus == ExitingStatus::kRunning) {
		gExitingStatus = ExitingStatus::kWantExit;
		syslog(LOG_INFO, "Exiting on internal request.");
		return LIZARDFS_STATUS_OK;
	} else {
		syslog(LOG_ERR, "Unable to exit on internal request.");
		return LIZARDFS_ERROR_NOTPOSSIBLE;
	}
}

void eventloop_want_to_reload() {
	gReloadRequested = true;
}

void eventloop_run() {
	uint32_t prevtime  = 0;
	uint64_t prevmtime = 0;
	eloopentry *eloopit;
	ceentry *ceit;
	weentry *weit;
	rlentry *rlit;
	std::vector<pollfd> pdesc;
	int i;

	while (gExitingStatus != ExitingStatus::kDoExit) {
		pdesc.clear();
		for (auto &pollit: gPollEntries) {
			pollit.desc(pdesc);
		}
		i = poll(pdesc.data(),pdesc.size(), nextPollNonblocking ? 0 : 50);
		nextPollNonblocking = false;
		eventloop_updatetime();
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

void eventloop_updatetime() {
	struct timeval tv;

	gettimeofday(&tv,NULL);
	usecnow = tv.tv_sec * uint64_t(1000000) + tv.tv_usec;
	now = tv.tv_sec;
}
