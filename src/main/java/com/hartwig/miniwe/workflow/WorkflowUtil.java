package com.hartwig.miniwe.workflow;

import com.hartwig.miniwe.kubernetes.KubernetesUtil;
import com.hartwig.miniwe.miniwdl.ExecutionDefinition;
import com.hartwig.miniwe.miniwdl.WorkflowDefinition;

public final class WorkflowUtil {
    public static String getRunName(ExecutionDefinition executionDefinition) {
        return KubernetesUtil.toValidRFC1123Label(executionDefinition.workflow(),
                executionDefinition.version(),
                executionDefinition.name());
    }

    public static String getWorkflowName(WorkflowDefinition workflowDefinition) {
        return KubernetesUtil.toValidRFC1123Label(workflowDefinition.name(), workflowDefinition.version());
    }

    public static String getWorkflowName(ExecutionDefinition executionDefinition) {
        return KubernetesUtil.toValidRFC1123Label(executionDefinition.workflow(), executionDefinition.version());
    }
}
