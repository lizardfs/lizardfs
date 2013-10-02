#include "deleattr.h"


DeleteEAttrTool::DeleteEAttrTool() : ExtendedAttributesTool("DELEATTR") {
}

void DeleteEAttrTool::usage() {
    fprintf(stderr,"delete objects extra attributes\n\nusage: %s [-nhHr] -f attrname [-f attrname ...] name [name ...]\n", this->m_ToolName.c_str() );
    print_numberformat_options();
    print_recursive_option();
    fprintf(stderr," -f attrname - specify attribute to delete\n");
    print_extra_attributes();
}


int DeleteEAttrTool::executeCommand() {

    if ( m_Parameters.eattr == 0 && m_Parameters.argc >= 1) {
        fprintf(stderr,"no attribute(s) to delete\n");
        this->usage();
    }

    int status = 0;
    while(m_Parameters.argc > 0) {
       if (set_eattr(*m_Parameters.argv, m_Parameters.eattr, (m_Parameters.rflag)?(SMODE_RMASK | SMODE_DECREASE) : SMODE_DECREASE) < 0) {
            status=1;
        }
       --m_Parameters.argc;
       ++m_Parameters.argv;
     }
    return status;
}


//const bool DeleteEAttrTool::m_DeleteEAttrToolRegistered = LizardTool::registerTool(new DeleteEAttrTool());
