#pragma once
#include "config.h"

#include <sys/types.h>

namespace LizardClient {

/**
 * Class containing arguments that are passed with every request to the filesystem
 */
struct Context {
	Context(uid_t uid, gid_t gid, pid_t pid, mode_t umask)
			: uid(uid), gid(gid), pid(pid), umask(umask) {
	}

	uid_t uid;
	gid_t gid;
	pid_t pid;
	mode_t umask;
};

} // namespace LizardClient
