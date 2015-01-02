#pragma once
#include "common/platform.h"

#include "common/exception.h"

LIZARDFS_CREATE_EXCEPTION_CLASS(ConfigurationException, Exception);
LIZARDFS_CREATE_EXCEPTION_CLASS(FilesystemException, Exception);
LIZARDFS_CREATE_EXCEPTION_CLASS(ParseException, Exception);
LIZARDFS_CREATE_EXCEPTION_CLASS(InitializeException, Exception);
LIZARDFS_CREATE_EXCEPTION_CLASS(ConnectionException, Exception);
