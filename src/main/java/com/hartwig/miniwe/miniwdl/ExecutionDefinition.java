package com.hartwig.miniwe.miniwdl;

import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@Value.Immutable
@Value.Style(jdkOnly = true)
@JsonDeserialize(as = ImmutableExecutionDefinition.class)
@JsonSerialize(as = ImmutableExecutionDefinition.class)
public interface ExecutionDefinition {

    /**
     * Name of the execution. Different executions should have different names.
     * If the name of the execution already exists, the run bucket will be reused.
     */
    String name();

    /**
     * Workflow version, version of the workflow to run this execution in.
     */
    String version();

    /**
     * Workflow name, should refer to an already existing workflow.
     */
    String workflow();

    /**
     * Parameters for the workflow. Each parameter here should exist in the workflow
     * as an input parameter.
     */
    Map<String, String> params();
}
