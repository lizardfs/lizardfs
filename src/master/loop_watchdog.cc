/*
   Copyright 2016 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"

#include "master/loop_watchdog.h"

volatile bool SignalLoopWatchdog::exit_loop_ = false;

void SignalLoopWatchdog::alarmHandler(int /*signal*/) {
	SignalLoopWatchdog::exit_loop_ = true;
}

bool SignalLoopWatchdog::kHandlerInitialized = SignalLoopWatchdog::initHandler();

#ifndef NDEBUG
int SignalLoopWatchdog::refcount_ = 0;
#endif
