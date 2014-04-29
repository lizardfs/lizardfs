#include <cstdio>

#include "common/cwrap.h"

void CFileCloser::operator()(FILE* file_) const {
	::std::fclose(file_);
}

