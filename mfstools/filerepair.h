#ifndef LIZARDFS_MFSTOOLS_FILEREPAIR_H_
#define LIZARDFS_MFSTOOLS_FILEREPAIR_H_

#include "humodebased.h"

class FileRepairTool : public HumodeBasedTool
{

protected:
    virtual int executeCommand();

public:
    FileRepairTool();
    virtual void usage();
//    static const bool m_FileRepairToolRegistered;
};

#endif //LIZARDFS_MFSTOOLS_FILEREPAIR_H_
