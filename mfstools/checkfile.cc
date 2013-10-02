#include "checkfile.h"

CheckFileTool::CheckFileTool() : HumodeBasedTool("CHECKFILE") {
}

void CheckFileTool::usage() {
    fprintf(stderr,"check files\n\nusage: %s [-nhH] name [name ...]\n", this->m_ToolName.c_str());
}

int CheckFileTool::executeCommand() {
    int status = 0;
    while(m_Parameters.first > 0) {
        if (check_file(*m_Parameters.second) < 0) {
            status = 1;
        }
        --m_Parameters.first;
        ++m_Parameters.second;
    }
    return status;
}


//const bool CheckFileTool::m_CheckFileToolRegistered = LizardTool::registerTool(new CheckFileTool());
