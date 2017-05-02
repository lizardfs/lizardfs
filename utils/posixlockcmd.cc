/*
   Copyright 2015 Skytechnology sp. z o.o.

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

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <map>
#include <signal.h>
#include <string>
#include <sys/file.h>
#include <unistd.h>

int fd;
int cmdno;
int cmd;
struct flock lock;
const char *path;

std::map<char,int> commands;
std::map<char, std::string> command_strings;

void initialize_globals() {
	lock.l_type = F_UNLCK;
	lock.l_whence = SEEK_SET;
	commands['r'] = F_RDLCK;
	commands['w'] = F_WRLCK;
	command_strings['r'] = "read ";
	command_strings['w'] = "write";
}

void sig_usr1_handler(int signo)
{
	(void) signo;
	fprintf(stdout, "%s unlock: %s\n", command_strings[cmdno].c_str(), path);
	fflush(stdout);

	lock.l_type = F_UNLCK;
	fcntl(fd, F_SETLKW, &lock);
}

void sig_usr2_handler(int signo)
{
	(void) signo;
	fprintf(stdout, "%s interrupted: %s\n", command_strings[cmdno].c_str(), path);
	fflush(stdout);
}

void sig_int_handler(int signo)
{
	(void) signo;
	fprintf(stdout, "%s closed: %s\n", command_strings[cmdno].c_str(), path);
	close(fd);
	fflush(stdout);
}

void register_handler(int signo, void (*handler)(int), int flags) {
	struct sigaction action;
	action.sa_handler = handler;
	sigemptyset (&action.sa_mask);
	action.sa_flags = flags;
	if (sigaction (signo, &action, NULL) < 0) {
		fprintf(stderr, "Failed to register handler for signal %i\n", signo);
		exit(EXIT_FAILURE);
	}
}

int main(int argc, char *argv[])
{
	initialize_globals();
	int ret;

	register_handler(SIGUSR1, sig_usr1_handler,SA_RESTART);
	register_handler(SIGUSR2, sig_usr2_handler,0);
	register_handler(SIGINT, sig_int_handler,0);

	if (argc < 3 || argc > 5 || argc == 4) {
		fprintf(stderr, "Usage: posixlockcmd path [r/w/u]\n");
		return EXIT_FAILURE;
	}

	if (strlen(argv[2]) != 1) {
		fprintf(stderr, "Available parameters: r w u\n");
		return EXIT_FAILURE;
	}

	cmdno = argv[2][0];

	if (cmdno != 'r' && cmdno != 'w') {
		fprintf(stderr, "Available parameters: r w\n");
	}

	cmd = commands[cmdno];

	path = argv[1];
	fd = open(path, O_RDWR);

	if (fd < 0) {
		fprintf(stderr, "Failed to open file %s\n", path);
		return EXIT_FAILURE;
	}

	fprintf(stdout, "%s open:   %s\n", command_strings[cmdno].c_str(), path);
	fflush(stdout);

	lock.l_pid = getpid();
	lock.l_type = cmd;
	if (argc == 5) {
		lock.l_start = std::stoul(argv[3]);
		lock.l_len = std::stoul(argv[4]);
	}
	ret = fcntl(fd, F_SETLKW, &lock);

	if (ret == -1) {
		fprintf(stderr, "fcntl() failed: %s\n", strerror(errno));
	}

	fprintf(stdout, "%s lock:   %s\n", command_strings[cmdno].c_str(), path);
	fflush(stdout);
	pause();

	return EXIT_SUCCESS;
}
