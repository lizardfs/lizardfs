#pragma once

#include "config.h"

#include "common/exceptions.h"

LIZARDFS_CREATE_EXCEPTION_CLASS(WriteException, Exception);
LIZARDFS_CREATE_EXCEPTION_CLASS(RecoverableWriteException, WriteException);
LIZARDFS_CREATE_EXCEPTION_CLASS(UnrecoverableWriteException, WriteException);
LIZARDFS_CREATE_EXCEPTION_CLASS(NoValidCopiesWriteException, RecoverableWriteException);
