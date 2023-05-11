package com.hartwig.miniwe.kubernetes;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

import com.hartwig.miniwe.workflow.ExecutionStage;
import com.hartwig.miniwe.workflow.StageScheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.client.KubernetesClient;

public class KubernetesStageScheduler implements StageScheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesStageScheduler.class);
    public static final int DEFAULT_STORAGE_SIZE_GI = 1;

    private final Collection<StageRun> completedStages = new ConcurrentLinkedQueue<>();
    private final String namespace;
    private final ExecutorService executor;
    private final KubernetesClient kubernetesClient;
    private final String serviceAccountName;
    private final StorageProvider storageProvider;

    public KubernetesStageScheduler(final String namespace, final ExecutorService executor, final KubernetesClient kubernetesClient,
            final String serviceAccountName, final StorageProvider storageProvider) {
        this.serviceAccountName = serviceAccountName;
        this.namespace = namespace;
        this.executor = executor;
        this.kubernetesClient = kubernetesClient;
        this.storageProvider = storageProvider;
    }

    @Override
    public CompletableFuture<Boolean> schedule(final ExecutionStage executionStage) {
        return CompletableFuture.supplyAsync(() -> {
            var definition = new StageDefinition(executionStage, namespace, DEFAULT_STORAGE_SIZE_GI, serviceAccountName, storageProvider);
            var run = definition.submit(kubernetesClient);
            try {
                run.start();
                var result = run.waitUntilComplete();
                LOGGER.info("Stage [{}] completed with status [{}]", definition.getStageName(), result ? "Success" : "Failed");
                return result;
            } catch (Exception e) {
                LOGGER.error("Stage [{}] failed with", definition.getStageName(), e);
                return false;
            } finally {
                completedStages.add(run);
            }
        }, executor);
    }

    public void cleanup() {
        // TODO: Also clean up running resources...
        for (final StageRun completedStage : this.completedStages) {
            completedStage.cleanup();
        }
    }
}
