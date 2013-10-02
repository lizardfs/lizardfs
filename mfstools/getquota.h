#ifndef LIZARDFS_MFSTOOLS_GETQUOTA_H_
#define LIZARDFS_MFSTOOLS_GETQUOTA_H_

#include "humodebased.h"

class GetQuotaTool : public HumodeBasedTool
{

protected:
    virtual int executeCommand();

public:
    GetQuotaTool();
    virtual void usage();
   // static const bool m_GetQuotaToolRegistered;
};

#endif //LIZARDFS_MFSTOOLS_GETQUOTA_H_
