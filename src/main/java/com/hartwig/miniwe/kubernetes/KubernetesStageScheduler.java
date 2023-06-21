package com.hartwig.miniwe.kubernetes;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import com.hartwig.miniwe.ExecutorUtil;
import com.hartwig.miniwe.miniwdl.ExecutionDefinition;
import com.hartwig.miniwe.workflow.ExecutionStage;
import com.hartwig.miniwe.workflow.StageScheduler;
import com.hartwig.miniwe.workflow.WorkflowUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesStageScheduler implements StageScheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesStageScheduler.class);
    public static final int DEFAULT_STORAGE_SIZE_GI = 1;
    private static final int MAX_CONCURRENT_STAGES = 128;

    private final ConcurrentMap<ExecutionStage, StageRun> stageRunByExecutionStage = new ConcurrentHashMap<>();
    private final String namespace;
    private final ExecutorService executor;
    private final KubernetesClientWrapper kubernetesClient;
    private final String serviceAccountName;
    private final StorageProvider storageProvider;

    public KubernetesStageScheduler(String namespace, KubernetesClientWrapper kubernetesClient, String serviceAccountName,
            StorageProvider storageProvider) {
        this.serviceAccountName = serviceAccountName;
        this.namespace = namespace;
        this.executor = ExecutorUtil.createExecutorService(MAX_CONCURRENT_STAGES, "stage-run-thread-%d");
        this.kubernetesClient = kubernetesClient;
        this.storageProvider = storageProvider;
    }

    @Override
    public synchronized CompletableFuture<Boolean> schedule(ExecutionStage executionStage) {
        if (stageRunByExecutionStage.containsKey(executionStage)) {
            throw new IllegalStateException(String.format("Cannot schedule stage with name '%s' since it already exists",
                    executionStage.runName()));
        }
        var definition = new StageDefinition(executionStage, namespace, DEFAULT_STORAGE_SIZE_GI, serviceAccountName, storageProvider);
        var stageRun = definition.createStageRun(kubernetesClient);
        stageRunByExecutionStage.put(executionStage, stageRun);
        return CompletableFuture.supplyAsync(() -> {
            try {
                stageRun.start();
                var success = stageRun.waitUntilComplete();
                LOGGER.info("[{}] Stage completed with status '{}'", definition.getStageName(), success ? "Success" : "Failed");
                if (success) {
                    LOGGER.info("[{}] Cleaning up resources...", definition.getStageName());
                    stageRun.cleanup();
                    stageRunByExecutionStage.remove(executionStage);
                    LOGGER.info("[{}] Cleaned up resources for stage", definition.getStageName());
                }
                return success;
            } catch (Exception e) {
                LOGGER.error("[{}] Stage failed with", definition.getStageName(), e);
                return false;
            }
        }, executor);
    }

    public synchronized void deleteStagesForRun(ExecutionDefinition executionDefinition) {
        for (var iterator = stageRunByExecutionStage.entrySet().iterator(); iterator.hasNext(); ) {
            final var entries = iterator.next();
            if (entries.getKey().runName().equals(WorkflowUtil.getRunName(executionDefinition))) {
                entries.getValue().cleanup();
                iterator.remove();
            }
        }
    }
}
