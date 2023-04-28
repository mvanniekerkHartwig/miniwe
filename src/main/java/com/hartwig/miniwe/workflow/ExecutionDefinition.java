package com.hartwig.miniwe.workflow;

import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@Value.Style(jdkOnly = true)
@JsonDeserialize(as = ImmutableExecutionDefinition.class)
public interface ExecutionDefinition {
    String name();

    Map<String, String> params();
}
