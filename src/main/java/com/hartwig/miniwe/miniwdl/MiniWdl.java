package com.hartwig.miniwe.miniwdl;

import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

/**
 * Mini Workflow Definition Language
 */
@Value.Immutable
@Value.Style(jdkOnly = true)
@JsonDeserialize(as = ImmutableMiniWdl.class)
public interface MiniWdl {
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
     * List of stages.
     */
    List<Stage> stages();
}
