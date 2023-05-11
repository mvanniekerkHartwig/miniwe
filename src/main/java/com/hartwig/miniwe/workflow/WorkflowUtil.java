package com.hartwig.miniwe.workflow;

import com.hartwig.miniwe.kubernetes.KubernetesUtil;
import com.hartwig.miniwe.miniwdl.ExecutionDefinition;

public final class WorkflowUtil {
    public static String getRunName(ExecutionDefinition executionDefinition) {
        return KubernetesUtil.toValidRFC1123Label(executionDefinition.workflow(), executionDefinition.name());
    }
}
