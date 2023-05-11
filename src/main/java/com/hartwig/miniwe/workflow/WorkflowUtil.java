package com.hartwig.miniwe.workflow;

import com.hartwig.miniwe.kubernetes.KubernetesUtil;
import com.hartwig.miniwe.miniwdl.ExecutionDefinition;
import com.hartwig.miniwe.miniwdl.MiniWdl;

public final class WorkflowUtil {
    public static String getRunName(MiniWdl pipeline, ExecutionDefinition executionDefinition) {
        return KubernetesUtil.toValidRFC1123Label(pipeline.name(), executionDefinition.name());
    }
}
