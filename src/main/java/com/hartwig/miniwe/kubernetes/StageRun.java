package com.hartwig.miniwe.kubernetes;

/**
 * Representation of the run of a stage
 */
interface StageRun {
    /**
     * Start the run of a stage
     */
    void start();

    /**
     * Block until the run is complete.
     * @return true if the run succeeded
     */
    boolean waitUntilComplete();

    /**
     * Clean up the kubernetes resources.
     */
    void cleanup();
}
