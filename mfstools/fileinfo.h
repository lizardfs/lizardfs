#ifndef LIZARDFS_MFSTOOLS_FILEINFO_H_
#define LIZARDFS_MFSTOOLS_FILEINFO_H_

#include "lizardtools.h"

class FileInfoTool : public LizardCommand<std::pair<int, char**>> {

protected:
    virtual int executeCommand();

public:
    //static const bool m_FileInfoToolRegistered;
    FileInfoTool();
    virtual void usage();
};


#endif //LIZARDFS_MFSTOOLS_FILEINFO_H_
