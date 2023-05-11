package com.hartwig.miniwe.kubernetes;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.client.KubernetesClient;

public class KubernetesEnvironment {
    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesEnvironment.class);

    private final String namespace;
    private final ExecutorService executor;
    private final KubernetesClient kubernetesClient;
    private final String serviceAccountName;

    private final Map<String, KubernetesStageScheduler> stageSchedulerToRunName = new HashMap<>();

    public KubernetesEnvironment(final String namespace, final ExecutorService executor, final KubernetesClient kubernetesClient,
            final String serviceAccountName) {
        this.namespace = namespace;
        this.executor = executor;
        this.kubernetesClient = kubernetesClient;
        this.serviceAccountName = serviceAccountName;
    }

    public KubernetesStageScheduler findOrCreateScheduler(final String runName, final StorageProvider storageProvider) {
        if (stageSchedulerToRunName.containsKey(runName)) {
            return stageSchedulerToRunName.get(runName);
        }
        var kubernetesStageScheduler =
                new KubernetesStageScheduler(namespace, executor, kubernetesClient, serviceAccountName, storageProvider);
        stageSchedulerToRunName.put(runName, kubernetesStageScheduler);
        return kubernetesStageScheduler;
    }

    public void deleteScheduledResources(final String runName) {
        var kubernetesStageScheduler = stageSchedulerToRunName.remove(runName);
        if (kubernetesStageScheduler == null) {
            LOGGER.warn("Could not find kubernetes scheduled resources to delete for run name '{}'", runName);
            return;
        }
        kubernetesStageScheduler.cleanup();
    }
}
