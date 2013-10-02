#ifndef LIZARDFS_MFSTOOLS_SETTRASHTIME_H_
#define LIZARDFS_MFSTOOLS_SETTRASHTIME_H_

#include "lizardtools.h"

struct SetTrashTimeParams {
    int argc;
    char** argv;
    uint32_t trashtime;
    int rflag;
    uint8_t smode;

    SetTrashTimeParams() : argc(0), trashtime(0), rflag(0), smode(SMODE_SET) {
    }
};

class SetTrashTimeTool : public LizardCommand<SetTrashTimeParams> {

protected:
    virtual int executeCommand();
    virtual void parseOptions(const int& pArgc, char** pArgv);

public:
//    static const bool m_SetTrashTimeToolRegistered;
    SetTrashTimeTool();
    SetTrashTimeTool(const std::string& pToolName, const bool& pDeprecated);
    virtual void usage();
};

#endif //LIZARDFS_MFSTOOLS_SETTRASHTIME_H_
