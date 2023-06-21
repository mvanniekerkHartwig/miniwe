package com.hartwig.miniwe.kubernetes;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.batch.v1.Job;

class StageRun {
    private static final Logger LOGGER = LoggerFactory.getLogger(StageRun.class);
    private static final int STAGE_TIMEOUT_MINUTES = 15;

    private final PersistentVolumeClaim persistentVolumeClaim;
    private final Job job;
    private final Job onCompleteCopyJob;

    private final KubernetesClientWrapper client;

    StageRun(PersistentVolumeClaim persistentVolumeClaim, Job job, Job onCompleteCopyJob, KubernetesClientWrapper client) {
        this.persistentVolumeClaim = persistentVolumeClaim;
        this.job = job;
        this.onCompleteCopyJob = onCompleteCopyJob;
        this.client = client;
    }

    void start() {
        cleanup();
        client.create(persistentVolumeClaim);
        client.create(job);
    }

    boolean waitUntilComplete() {
        var jobSucceeded = client.waitUntilJobComplete(job, STAGE_TIMEOUT_MINUTES, TimeUnit.MINUTES);
        if (!jobSucceeded) {
            return false;
        }
        client.create(onCompleteCopyJob);
        return client.waitUntilJobComplete(onCompleteCopyJob, STAGE_TIMEOUT_MINUTES, TimeUnit.MINUTES);
    }

    void cleanup() {
        client.deleteIfExists(job);
        client.deleteIfExists(onCompleteCopyJob);
        client.deleteIfExists(persistentVolumeClaim);
    }
}
