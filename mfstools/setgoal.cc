#include "setgoal.h"

SetGoalTool::SetGoalTool() : LizardCommand("SETGOAL") {
}

SetGoalTool::SetGoalTool(const std::string& pToolName, const bool& pDeprecated) :
LizardCommand(pToolName, pDeprecated) {

}

void SetGoalTool::usage() {
    fprintf(stderr,"set objects goal (desired number of copies)\n\nusage:  \
                    %s [-nhHr] GOAL[-|+] name [name ...]\n", this->m_ToolName.c_str() );
    print_numberformat_options();
    print_recursive_option();
    fprintf(stderr," GOAL+ - increase goal to given value\n");
    fprintf(stderr," GOAL- - decrease goal to given value\n");
    fprintf(stderr," GOAL - just set goal to given value\n");
}

void SetGoalTool::parseOptions(const int& pArgc, char** pArgv) {
    m_Parameters = SetGoalParams();
    m_Parameters.argc = int(pArgc);
    m_Parameters.argv = pArgv;

    if(0 == m_Parameters.argc - optind) {
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
            this->m_Parameters.rflag = 1;
            break;
        }
    }

    m_Parameters.argc -= optind;
    m_Parameters.argv += optind;

    char *p = m_Parameters.argv[0];
    if (p[0]>'0' && p[0]<='9' && (p[1]=='\0' || ((p[1]=='-' || p[1]=='+') && p[2]=='\0'))) {
        m_Parameters.goal = p[0]-'0';
        if (p[1]=='-') {
            this->m_Parameters.smode=SMODE_DECREASE;
        } else if (p[1]=='+') {
            this->m_Parameters.smode=SMODE_INCREASE;
        }
    } else {
        fprintf(stderr,"goal should be given as a digit between 1 and 9 optionally folowed by '-' or '+'\n");
        this->usage();
    }
    --m_Parameters.argc;
    ++m_Parameters.argv;
}

int SetGoalTool::executeCommand() {
    int status = 0;
    while(m_Parameters.argc > 0) {
        if (set_goal(*m_Parameters.argv, m_Parameters.goal,
                     (m_Parameters.rflag)?
                     (m_Parameters.smode | SMODE_RMASK):m_Parameters.smode) < 0) {
            status = 1;
        }
        --m_Parameters.argc;
        ++m_Parameters.argv;
     }
    return status;
}

//const bool SetGoalTool::m_SetGoalToolRegistered = LizardTool::registerTool(new SetGoalTool());
