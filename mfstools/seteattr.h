#ifndef LIZARDFS_MFSTOOLS_SETEATTR_H_
#define LIZARDFS_MFSTOOLS_SETEATTR_H_

#include "extendedattr.h"

class SetEAttrTool : public ExtendedAttributesTool {

protected:
        virtual int executeCommand();

    public:
        SetEAttrTool();
        virtual void usage();
        //static const bool m_SetEAttrToolRegistered;
};

#endif //LIZARDFS_MFSTOOLS_SETEATTR_H_
