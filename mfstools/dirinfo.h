#ifndef LIZARDFS_MFSTOOLS_DIRINFO_H_
#define LIZARDFS_MFSTOOLS_DIRINFO_H_

#include "humodebased.h"

class DirInfoTool : public HumodeBasedTool
{

protected:
    virtual int executeCommand();

public:
    DirInfoTool();
    virtual void usage();
//    static const bool m_DirInfoToolRegistered;
};

#endif //LIZARDFS_MFSTOOLS_DIRINFO_H_
