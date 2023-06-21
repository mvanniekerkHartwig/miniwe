package com.hartwig.miniwe.miniwdl;

import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.hartwig.miniwe.miniwdl.ImmutableExecutionDefinition;

import org.immutables.value.Value;

@Value.Immutable
@Value.Style(jdkOnly = true)
@JsonDeserialize(as = ImmutableExecutionDefinition.class)
public interface ExecutionDefinition {
    String name();

    String workflow();

    Map<String, String> params();
}
