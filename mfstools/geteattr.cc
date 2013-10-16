#include "geteattr.h"

GetExtraAttrTool::GetExtraAttrTool() : LizardCommand("GETEATTR") {
}

void GetExtraAttrTool::usage() {
    fprintf(stderr,"get objects extra attributes\n\nusage: %s [-nhHr] name [name ...]\n" ,this->m_ToolName.c_str() );
    print_numberformat_options();
    print_recursive_option();
}

void GetExtraAttrTool::parseOptions(const int& pArgc, char** pArgv) {
    m_Parameters = ExtAttrParams();

    m_Parameters.argc = int(pArgc);
    m_Parameters.argv = pArgv;

    int ch = 0;
    while ((ch = getopt(m_Parameters.argc, m_Parameters.argv, "rnhH")) != -1 ) {
        switch(ch) {
        case 'n':
            LizardTool::m_Humode=0;
            break;
        case 'h':
            LizardTool::m_Humode=1;
            break;
        case 'H':
            LizardTool::m_Humode=2;
            break;
        case 'r':
            m_Parameters.rflag=1;
            break;
        }
    }
    m_Parameters.argc -= optind;
    m_Parameters.argv += optind;
}

int GetExtraAttrTool::executeCommand() {
    int status = 0;

    while(m_Parameters.argc > 0) {
        if (get_eattr(*m_Parameters.argv,(m_Parameters.rflag) ? GMODE_RECURSIVE : GMODE_NORMAL) < 0) {
            status=1;
        }
        --m_Parameters.argc;
        ++m_Parameters.argv;
    }
    return status;
}

//const bool GetExtraAttrTool::m_GetExtraAttrToolRegistered = LizardTool::registerTool(new GetExtraAttrTool());
