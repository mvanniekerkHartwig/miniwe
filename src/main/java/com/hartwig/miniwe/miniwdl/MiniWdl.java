package com.hartwig.miniwe.miniwdl;

import java.util.List;

/**
 * Mini Workflow Definition Language
 */
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
