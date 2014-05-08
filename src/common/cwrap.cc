#include "config.h"
#include "common/cwrap.h"

void CFileCloser::operator()(FILE* file) const {
	::std::fclose(file);
}

void CDirCloser::operator()(DIR* dir) const {
	closedir(dir);
}
