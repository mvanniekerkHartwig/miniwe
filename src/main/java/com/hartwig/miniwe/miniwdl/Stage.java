package com.hartwig.miniwe.miniwdl;

import java.util.List;
import java.util.Optional;

public interface Stage {
    /**
     * Stage name
     */
    String name();

    /**
     * Docker image name
     */
    String image();

    /**
     * Docker image SemVer
     */
    String version();

    /**
     * All stages whose outputs should be provided as input for this stage.
     */
    List<String> inputStages();

    /**
     * Arguments passed into the docker container directly after the entrypoint
     */
    Optional<String> arguments();

    /**
     * Entrypoint. Default is the docker entrypoint.
     */
    Optional<String> entrypoint();
}
