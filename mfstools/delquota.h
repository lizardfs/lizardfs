#ifndef LIZARDFS_MFSTOOLS_DELQUOTA_H_
#define LIZARDFS_MFSTOOLS_DELQUOTA_H_

#include "lizardtools.h"

struct DelQuotaParams
{
    int argc;
    char** argv;
    uint8_t qflags;
    DelQuotaParams() : argc(0), qflags(0) {}
};


class DelQuotaTool : public LizardCommand<DelQuotaParams> {
    protected:
        virtual int executeCommand();
        virtual void parseOptions(const int& pArgc, char** pArgv);

    public:
//        static const bool m_DelQuotaToolRegistered;
        DelQuotaTool();
        virtual void usage();
};

#endif //LIZARDFS_MFSTOOLS_DELQUOTA_H_
