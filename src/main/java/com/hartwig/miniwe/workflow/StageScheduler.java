package com.hartwig.miniwe.workflow;

import java.util.concurrent.CompletableFuture;

public interface StageScheduler {
    CompletableFuture<Boolean> schedule(ExecutionStage executionStage);
}
