package com.hartwig.miniwe.kubernetes;

import java.util.concurrent.CompletableFuture;

import com.hartwig.miniwe.miniwdl.Stage;
import com.hartwig.miniwe.workflow.StageScheduler;

public class KubernetesStageScheduler implements StageScheduler {

    @Override
    public CompletableFuture<Boolean> schedule(final Stage stage) {
        return null;
    }
}
