#ifndef LIZARDFS_MFSTOOLS_CHECKFILE_H_
#define LIZARDFS_MFSTOOLS_CHECKFILE_H_

#include "humodebased.h"

class CheckFileTool : public HumodeBasedTool
{

protected:
    virtual int executeCommand();

public:
    CheckFileTool();
    virtual void usage();
//    static const bool m_CheckFileToolRegistered;
};

#endif //LIZARDFS_MFSTOOLS_CHECKFILE_H_
