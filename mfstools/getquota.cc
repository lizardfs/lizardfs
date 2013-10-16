#include "getquota.h"

GetQuotaTool::GetQuotaTool() : HumodeBasedTool("GETQUOTA") {
}

void GetQuotaTool::usage() {
    fprintf(stderr,"get quota for given directory (directories)\n\nusage: %s [-nhH] dirname [dirname ...]\n", this->m_ToolName.c_str() );
    print_numberformat_options();
}

int GetQuotaTool::executeCommand() {
    int status = 0;
    while(m_Parameters.first > 0) {
        if (quota_control(*m_Parameters.second, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0) < 0) {
            status=1;
        }
        --m_Parameters.first;
        ++m_Parameters.second;
    }
    return status;
}

//const bool GetQuotaTool::m_GetQuotaToolRegistered = LizardTool::registerTool(new GetQuotaTool());

