#include "toolscommon.h"

static bool registerTools() {
    LizardTool::registerTool(new SetQuotaTool());
    LizardTool::registerTool(new SetTrashTimeTool());
    LizardTool::registerTool(new SetQuotaTool());
    LizardTool::registerTool(new SetGoalTool());
    LizardTool::registerTool(new SetEAttrTool());
    LizardTool::registerDeprecatedTool("mfsrsettrashtime", new SetTrashTimeTool("rsettrashtime", true));
    LizardTool::registerDeprecatedTool("mfsrsetgoal", new SetGoalTool("rsetgoal", true));
    LizardTool::registerDeprecatedTool("mfsrgettrashtime", new GetTrashTimeTool("rgettrashtime", true));
    LizardTool::registerDeprecatedTool("mfsrgetgoal", new GetGoalTool("rgetgoal", true));
    LizardTool::registerTool(new MakeSnapshotTool());
    LizardTool::registerTool(new GetTrashTimeTool());
    LizardTool::registerTool(new GetQuotaTool());
    LizardTool::registerTool(new GetGoalTool());
    LizardTool::registerTool(new GetExtraAttrTool());
    LizardTool::registerTool(new FileRepairTool());
    LizardTool::registerTool(new FileInfoTool());
    LizardTool::registerTool(new DirInfoTool());
    LizardTool::registerTool(new DelQuotaTool());
    LizardTool::registerTool(new DeleteEAttrTool());
    LizardTool::registerTool(new CheckFileTool());
    LizardTool::registerTool(new AppendChunksTool());
    LizardTool::registerTool(new LizardLinkTool());
    return true;
}

static const bool lizardToolsRegistered = registerTools();
