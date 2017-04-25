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

#pragma once

#include "common/platform.h"

#include <inttypes.h>
#include <string>
#include <vector>

#if defined(_WIN32)
  #include "winsock2.h"
#else
  #include <poll.h>
#endif


#define TIMEMODE_SKIP_LATE 0
#define TIMEMODE_RUN_LATE 1

enum class ExitingStatus {
	kRunning = 0,
	kWantExit = 1,
	kCanExit = 2,
	kDoExit = 3
};

extern ExitingStatus gExitingStatus;
extern bool gReloadRequested;

void eventloop_destructregister (void (*fun)(void));
void eventloop_canexitregister (int (*fun)(void));
void eventloop_wantexitregister (void (*fun)(void));
void eventloop_reloadregister (void (*fun)(void));
void eventloop_pollregister (void (*desc)(std::vector<pollfd>&),void (*serve)(const std::vector<pollfd>&));
void eventloop_eachloopregister (void (*fun)(void));

/*! \brief Register handler for recurring event.
 *
 * \param mode Event mode. Can be one of
 *                TIMEMODE_SKIP_LATE - if the event engine is late then we skip to the next time
 *                                     divisible by seconds.
 *                TIMEMODE_RUN_LATE  - can be executed even if the event engine is late (so time
 *                                     isn't divisible by seconds).
 * \param seconds when event should be run (event is executed at time divisible by
 *                value of seconds).
 * \param offset  if greater than 0 then event is executed offset seconds after time divisible by
 *                value of parameter seconds).
 * \param fun address of function to execute.
 * \return handle - handle to newly registered timed event.
 */
void *eventloop_timeregister(int mode, uint64_t seconds, uint64_t offset, void (*fun)(void));

/*! \brief Register handler for recurring event (millisecond precision).
 *
 * \param period  how often event should be run (in ms)
 * \param fun address of function to execute.
 * \return handle - handle to newly registered timed event.
 */
void *eventloop_timeregister_ms(uint64_t period, void (*fun)(void));

/*! \brief Make the next poll nonblocking
 */
void eventloop_make_next_poll_nonblocking();

/*! \brief Unregister previously registered timed event handler.
 *
 * \param handle - handle to currently registered timed event.
 */
void eventloop_timeunregister (void* handle);

/*! \brief Change previously registered timed event frequency and mode.
 *
 * \param handle  - handle to currently registered timed event.
 * \param mode    - event mode
 * \param seconds - event period
 * \param offset  - event offset
 * \return 0  - success
 *         -1 - error occurred
 */
int eventloop_timechange(void* handle, int mode, uint64_t seconds, uint64_t offset);

/*! \brief Change previously registered timed event frequency and mode.
 *
 * \param handle  - handle to currently registered timed event.
 * \param period - event period (in ms)
 * \return 0  - success
 *         -1 - error occurred
 */
int eventloop_timechange_ms(void* handle, uint64_t period);

/*! \brief Try to exit as if term signal was received.
 *
 * \return LIZARDFS_STATUS_OK if term sequence has been initialized
 *         LIZARDFS_ERROR_NOTPOSSIBLE if it was already initialized.
 */
uint8_t eventloop_want_to_terminate(void);

/*! \brief Request reloading the configuration.
 *
 * Reload will be performed after the current loop, so this function returns
 * before thereload actually happens.
 */
void eventloop_want_to_reload(void);

/*! \brief Run event loop. */
void eventloop_run();

/*! \brief Release resources used by event loop. */
void eventloop_release_resources(void);

/*! \brief Call function registered in function eventloop_destructregister. */
void eventloop_destruct();

/*! \brief Returns event loop time. The time is updated before call to handler functions
 * and doesn't change during handlers execution.
 *
 * \return time in milliseconds.
 */
uint32_t eventloop_time(void);

/*! \brief Returns event loop time. The time is updated before call to handler functions
 * and doesn't change during handlers execution.
 *
 * \return time in microseconds.
 */
uint64_t eventloop_utime(void);

/*! \brief Set time returned by functions eventloop_time and eventloop_utime
 * to current system time.
 */
void eventloop_updatetime();
