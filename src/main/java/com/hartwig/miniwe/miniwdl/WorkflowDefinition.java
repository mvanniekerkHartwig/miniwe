package com.hartwig.miniwe.miniwdl;

import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.hartwig.miniwe.kubernetes.KubernetesUtil;

import org.immutables.value.Value;

@Value.Immutable
@Value.Style(jdkOnly = true)
@JsonDeserialize(as = ImmutableWorkflowDefinition.class)
@JsonSerialize(as = ImmutableWorkflowDefinition.class)
public interface WorkflowDefinition {
    /**
     * WDL name, should be unique
     */
    String name();

    /**
     * WDL version
     */
    String version();

    /**
     * Input parameters for this workflow definition
     */
    List<String> params();

    /**
     * List of secret environment variables for all stages.
     */
    List<String> secrets();

    /**
     * Input stages required for this workflow definition
     */
    List<String> inputStages();

    /**
     * List of stages.
     */
    List<Stage> stages();

    default String getWorkflowName() {
        return KubernetesUtil.toValidRFC1123Label(name(), version());
    }

    static ImmutableWorkflowDefinition.Builder builder() {
        return ImmutableWorkflowDefinition.builder();
    }
}
