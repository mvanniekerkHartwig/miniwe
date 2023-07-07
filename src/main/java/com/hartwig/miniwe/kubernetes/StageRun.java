package com.hartwig.miniwe.kubernetes;

/**
 * Representation of the run of a stage
 */
interface StageRun {
    /**
     * Start the run of a stage, and block until the stage is complete
     * @return true if the run succeeded
     */
    boolean start();

    /**
     * Clean up the run resources.
     */
    void cleanup();
}
