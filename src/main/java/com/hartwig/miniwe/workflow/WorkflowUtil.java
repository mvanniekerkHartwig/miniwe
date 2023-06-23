package com.hartwig.miniwe.workflow;

import com.hartwig.miniwe.kubernetes.KubernetesUtil;
import com.hartwig.miniwe.miniwdl.ExecutionDefinition;
import com.hartwig.miniwe.miniwdl.WorkflowDefinition;

public final class WorkflowUtil {
    public static String getBucketName(String runName) {
        return KubernetesUtil.toValidRFC1123Label("run", runName);
    }
}
