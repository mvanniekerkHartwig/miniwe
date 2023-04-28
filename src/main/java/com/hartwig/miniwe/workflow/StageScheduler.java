package com.hartwig.miniwe.workflow;

import java.util.concurrent.CompletableFuture;

import com.hartwig.miniwe.miniwdl.Stage;

public interface StageScheduler {
    CompletableFuture<Boolean> schedule(String stageName);
}
