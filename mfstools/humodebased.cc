#include "humodebased.h"

HumodeBasedTool::HumodeBasedTool(const std::string& pToolName) : LizardCommand(pToolName) {
}

void HumodeBasedTool::parseOptions(const int& pArgc, char** pArgv) {
    m_Parameters.first = int(pArgc);
    m_Parameters.second = pArgv;
    int ch = 0;
    while ((ch = getopt(m_Parameters.first, m_Parameters.second, "nhH")) != -1) {
        switch(ch) {
        case 'n':
            LizardTool::m_Humode = 0;
            break;
        case 'h':
            LizardTool::m_Humode = 1;
            break;
        case 'H':
            LizardTool::m_Humode = 2;
            break;
        }
    }
    m_Parameters.first -= optind;
    m_Parameters.second += optind;
}
