package com.hartwig.miniwe.workflow;

import static com.hartwig.miniwe.kubernetes.KubernetesUtil.toValidRFC1123Label;

import com.hartwig.miniwe.miniwdl.ExecutionDefinition;
import com.hartwig.miniwe.miniwdl.WorkflowDefinition;

public final class WorkflowUtil {
    public static String getRunName(ExecutionDefinition executionDefinition) {
        return toValidRFC1123Label(executionDefinition.workflow(), executionDefinition.version(), executionDefinition.name());
    }

    @SuppressWarnings("unused")
    public static String getBucketName(ExecutionDefinition executionDefinition) {
        return getBucketName(getRunName(executionDefinition));
    }

    public static String getBucketName(String runName) {
        return toValidRFC1123Label("run", runName);
    }

    public static String getWorkflowName(WorkflowDefinition workflowDefinition) {
        return toValidRFC1123Label(workflowDefinition.name(), workflowDefinition.version());
    }

    public static String getWorkflowName(ExecutionDefinition executionDefinition) {
        return toValidRFC1123Label(executionDefinition.workflow(), executionDefinition.version());
    }
}
