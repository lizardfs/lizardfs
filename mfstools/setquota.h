#ifndef LIZARDFS_MFSTOOLS_SETQUOTA_H_
#define LIZARDFS_MFSTOOLS_SETQUOTA_H_

#include "lizardtools.h"

struct SetQuotaParams {
    int argc;
    char** argv;
    uint8_t qflags;
    uint32_t sinodes;
    uint64_t slength;
    uint64_t ssize;
    uint64_t srealsize;
    uint32_t hinodes;
    uint64_t hlength;
    uint64_t hsize;
    uint64_t hrealsize;

    SetQuotaParams() : argc(0), qflags(0), sinodes(0), slength(0), ssize(0), srealsize(0),
        hinodes(0), hlength(0), hsize(0), hrealsize(0)
    {}
};

class SetQuotaTool : public LizardCommand<SetQuotaParams> {
protected:
    virtual int executeCommand();
    virtual void parseOptions(const int& pArgc, char** pArgv);

public:
    //static const bool m_SetQuotaToolRegistered;
    SetQuotaTool();
    virtual void usage();
};

#endif //LIZARDFS_MFSTOOLS_SETQUOTA_H_
