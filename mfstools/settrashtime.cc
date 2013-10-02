#include "settrashtime.h"

SetTrashTimeTool::SetTrashTimeTool() : LizardCommand("SETTRASHTIME") {
}

SetTrashTimeTool::SetTrashTimeTool(const std::string& pToolName, const bool& pDeprecated) :
                  LizardCommand(pToolName, pDeprecated) {
}

void SetTrashTimeTool::usage() {
    fprintf(stderr,"set objects trashtime (how many seconds file should be left in trash)\n\nusage: %s [-nhHr] SECONDS[-|+] name [name ...]\n", this->m_ToolName.c_str() );
    print_numberformat_options();
    print_recursive_option();
    fprintf(stderr," SECONDS+ - increase trashtime to given value\n");
    fprintf(stderr," SECONDS- - decrease trashtime to given value\n");
    fprintf(stderr," SECONDS - just set trashtime to given value\n");
}

void SetTrashTimeTool::parseOptions(const int& pArgc, char** pArgv) {
    m_Parameters = SetTrashTimeParams();
    m_Parameters.argc = int(pArgc);
    m_Parameters.argv = pArgv;

    if(0 == this->m_Parameters.argc - optind) {
        this->usage();
    }

    int ch = 0;
    while ((ch = getopt(m_Parameters.argc, m_Parameters.argv,"rnhH")) != -1 ) {
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
            m_Parameters.rflag = 1;
            break;
        }
    }

    m_Parameters.argc -= optind;
    m_Parameters.argv += optind;

    char *p = m_Parameters.argv[0];
    m_Parameters.trashtime = 0;
    while (p[0]>='0' && p[0]<='9') {
        m_Parameters.trashtime*=10;
        m_Parameters.trashtime+=(p[0]-'0');
        p++;
    }
    if (p[0]=='\0' || ((p[0]=='-' || p[0]=='+') && p[1]=='\0')) {
        if (p[0]=='-') {
            m_Parameters.smode=SMODE_DECREASE;
        } else if (p[0]=='+') {
            m_Parameters.smode=SMODE_INCREASE;
        }
    } else {
        fprintf(stderr,"trashtime should be given as number of seconds optionally folowed by '-' or '+'\n");
        this->usage();
    }
    m_Parameters.argc--;
    m_Parameters.argv++;
}

int SetTrashTimeTool::executeCommand() {
    int status = 0;
    while( m_Parameters.argc > 0) {
        if (set_trashtime(*m_Parameters.argv, m_Parameters.trashtime,(m_Parameters.rflag)?(m_Parameters.smode | SMODE_RMASK):m_Parameters.smode) < 0) {
            status = 1;
        }
        ++m_Parameters.argv;
        --m_Parameters.argc;
    }
    return status;
}

//const bool SetTrashTimeTool::m_SetTrashTimeToolRegistered = LizardTool::registerTool(new SetTrashTimeTool());
