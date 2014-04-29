#include "config.h"
#include "common/cwrap.h"

#include <cstdio>

void CFileCloser::operator()(FILE* file_) const {
	::std::fclose(file_);
}

