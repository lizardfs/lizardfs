#pragma once
#include "common/platform.h"

#include <sys/types.h>

#ifdef _WIN32
#include <cstdint>
typedef uint32_t uid_t;
typedef uint32_t gid_t;
#endif

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
