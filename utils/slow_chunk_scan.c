#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <dirent.h>
#include <dlfcn.h>
#include <pthread.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

typedef DIR *(*opendir_t)(const char *name);
typedef int (*readdir_r_t)(DIR *dirp, struct dirent *entry, struct dirent **result);
typedef int (*closedir_t)(DIR *dirp);

#define MAX_DIR_SCAN 5000
static pthread_mutex_t data_lock = PTHREAD_MUTEX_INITIALIZER;
static DIR *fast_dir[MAX_DIR_SCAN];

static const char *fast_path_contains[] = {"hdd_0_0", "hdd_1_0", "hdd_2_0", "hdd_3_0", 0};

DIR *opendir(const char *name) {
	static opendir_t _opendir = NULL;
	DIR *result;
	int contains_path, i;

	if (!_opendir) {
		_opendir = (opendir_t)dlsym(RTLD_NEXT, "opendir");
	}

	result = _opendir(name);
	contains_path = 0;
	for (i = 0; fast_path_contains[i]; ++i) {
		if (strstr(name, fast_path_contains[i])) {
			contains_path = 1;
			break;
		}
	}

	if (contains_path) {
		pthread_mutex_lock(&data_lock);
		for (i = 0; i < MAX_DIR_SCAN; ++i) {
			if (fast_dir[i] == NULL) {
				fast_dir[i] = result;
				break;
			}
		}
		pthread_mutex_unlock(&data_lock);
	}

	return result;
}

int readdir_r(DIR *dirp, struct dirent *entry, struct dirent **result) {
	static readdir_r_t _readdir_r = NULL;
	int i, slow;

	slow = 1;
	pthread_mutex_lock(&data_lock);
	for (i = 0; i < MAX_DIR_SCAN; ++i) {
		if (fast_dir[i] == dirp) {
			slow = 0;
		}
	}
	pthread_mutex_unlock(&data_lock);

	if (slow) {
		sleep(1);
	}

	if (!_readdir_r) {
		_readdir_r = (readdir_r_t)dlsym(RTLD_NEXT, "readdir_r");
	}

	return _readdir_r(dirp, entry, result);
}

int closedir(DIR *dirp) {
	static closedir_t _closedir = NULL;
	int i;

	if (!_closedir) {
		_closedir = (closedir_t)dlsym(RTLD_NEXT, "closedir");
	}

	pthread_mutex_lock(&data_lock);
	for (i = 0; i < MAX_DIR_SCAN; ++i) {
		if (fast_dir[i] == dirp) {
			fast_dir[i] = NULL;
			break;
		}
	}
	pthread_mutex_unlock(&data_lock);

	return _closedir(dirp);
}
