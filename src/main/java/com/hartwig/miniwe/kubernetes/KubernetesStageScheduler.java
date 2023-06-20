package com.hartwig.miniwe.kubernetes;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import com.hartwig.miniwe.miniwdl.ExecutionDefinition;
import com.hartwig.miniwe.workflow.ExecutionStage;
import com.hartwig.miniwe.workflow.StageScheduler;
import com.hartwig.miniwe.workflow.WorkflowUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesStageScheduler implements StageScheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesStageScheduler.class);
    public static final int DEFAULT_STORAGE_SIZE_GI = 1;

    private final ConcurrentMap<ExecutionStage, StageRun> stageRunByExecutionStage = new ConcurrentHashMap<>();
    private final String namespace;
    private final ExecutorService executor;
    private final KubernetesClientWrapper kubernetesClient;
    private final String serviceAccountName;
    private final StorageProvider storageProvider;

    public KubernetesStageScheduler(final String namespace, final ExecutorService executor, final KubernetesClientWrapper kubernetesClient,
            final String serviceAccountName, final StorageProvider storageProvider) {
        this.serviceAccountName = serviceAccountName;
        this.namespace = namespace;
        this.executor = executor;
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
                LOGGER.info("[{}] Starting stage", definition.getStageName());
                var result = stageRun.waitUntilComplete();
                LOGGER.info("[{}] Stage completed with status '{}'", definition.getStageName(), result ? "Success" : "Failed");
                return result;
            } catch (Exception e) {
                LOGGER.error("[{}] Stage failed with", definition.getStageName(), e);
                return false;
            }
        }, executor);
    }

    public void deleteStagesForRun(ExecutionDefinition executionDefinition) {
        for (var entries : stageRunByExecutionStage.entrySet()) {
            if (entries.getKey().runName().equals(WorkflowUtil.getRunName(executionDefinition))) {
                entries.getValue().cleanup();
            }
        }
    }
}
