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

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/file.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>

int fd;
int cmdno;
char cmd;
const char *path;

std::map<char,int> commands;
std::map<char, std::string> command_strings;

void initialize_globals() {
	commands['r'] = LOCK_SH;
	commands['w'] = LOCK_EX;
	command_strings['r'] = "read ";
	command_strings['w'] = "write";
}

void sig_usr1_handler(int signo)
{
	(void) signo;
	fprintf(stdout, "%s unlock: %s\n", command_strings[cmdno].c_str(), path);
	fflush(stdout);
	flock(fd, LOCK_UN);
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

	// arguments check
	if (argc != 3) {
		fprintf(stderr, "Usage: ./flock path [r/w]\n");
		return EXIT_FAILURE;
	}

	if (strlen(argv[2]) != 1) {
		fprintf(stderr, "Available parameters: r w\n");
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

	ret = flock(fd, cmd);

	if (ret == -1) {
		fprintf(stderr, "flock() failed: %s\n", strerror(errno));
	}

	fprintf(stdout, "%s lock:   %s\n", command_strings[cmdno].c_str(), path);
	fflush(stdout);
	pause();

	return EXIT_SUCCESS;
}
