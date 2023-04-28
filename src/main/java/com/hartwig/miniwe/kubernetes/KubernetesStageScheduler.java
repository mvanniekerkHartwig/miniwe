package com.hartwig.miniwe.kubernetes;

import java.util.concurrent.CompletableFuture;

import com.hartwig.miniwe.miniwdl.MiniWdl;
import com.hartwig.miniwe.miniwdl.Stage;
import com.hartwig.miniwe.workflow.ExecutionDefinition;
import com.hartwig.miniwe.workflow.StageScheduler;

public class KubernetesStageScheduler implements StageScheduler {

    private final String namespace;
    private final ExecutionDefinition executionDefinition;
    private final MiniWdl miniWdl;

    public KubernetesStageScheduler(final String namespace, final ExecutionDefinition executionDefinition, final MiniWdl miniWdl) {
        this.namespace = namespace;
        this.executionDefinition = executionDefinition;
        this.miniWdl = miniWdl;
    }

    @Override
    public CompletableFuture<Boolean> schedule(final String stageName) {
        return null;
    }
}
