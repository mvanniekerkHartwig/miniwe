package com.hartwig.miniwe.miniwdl;

import java.util.List;

import org.immutables.value.Value;

/**
 * Mini Workflow Definition Language
 */
@Value.Immutable
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

    static ImmutableMiniWdl.Builder builder() {
        return ImmutableMiniWdl.builder();
    }
}
