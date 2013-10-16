#include "gettrashtime.h"

GetTrashTimeTool::GetTrashTimeTool() : LizardCommand("GETTRASHTIME") {
}

GetTrashTimeTool::GetTrashTimeTool(const std::string& pToolName, const bool& pDeprecated) :
                  LizardCommand(pToolName, pDeprecated) {
}

void GetTrashTimeTool::usage() {
    fprintf(stderr,"get objects trashtime (how many seconds file should be left in trash)\n\nusage: %s [-nhHr] name [name ...]\n", this->m_ToolName.c_str() );
    print_numberformat_options();
    print_recursive_option();
}

void GetTrashTimeTool::parseOptions(const int& pArgc, char** pArgv) {
    m_Parameters = ToolParams();
    m_Parameters.argc = int(pArgc);
    m_Parameters.argv = pArgv;

    if(0 == m_Parameters.argc - optind) {
        this->usage();
    }

    int ch = 0;
    while ((ch = getopt(m_Parameters.argc, m_Parameters.argv,"rnhH")) != -1) {
        switch(ch) {
        case 'n':
            m_Humode = 0;
            break;
        case 'h':
            m_Humode = 1;
            break;
        case 'H':
            m_Humode = 2;
            break;
        case 'r':
            this->m_Parameters.rflag = 1;
            break;
        }
    }

    m_Parameters.argc -= optind;
    m_Parameters.argv += optind;
}

int GetTrashTimeTool::executeCommand() {
    int status = 0;
    while(m_Parameters.argc > 0) {
        if (get_trashtime(*m_Parameters.argv,(m_Parameters.rflag) ? GMODE_RECURSIVE:GMODE_NORMAL ) < 0) {
            status = 1;
        }
        --m_Parameters.argc;
        ++m_Parameters.argv;
     }
    return status;
}

//const bool GetTrashTimeTool::m_GetTrashTimeToolRegistered = LizardTool::registerTool(new GetTrashTimeTool());
