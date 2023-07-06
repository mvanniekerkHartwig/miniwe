package com.hartwig.miniwe.kubernetes;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.batch.v1.Job;

class OutputStageRun implements StageRun {
    private static final Logger LOGGER = LoggerFactory.getLogger(OutputStageRun.class);
    private static final int STAGE_TIMEOUT_MINUTES = 15;

    private final PersistentVolumeClaim persistentVolumeClaim;
    private final Job job;
    private final Job onCompleteCopyJob;
    private final Secret secret;

    private final BlockingKubernetesClient client;

    OutputStageRun(PersistentVolumeClaim persistentVolumeClaim, Job job, Job onCompleteCopyJob, Secret secret,
            BlockingKubernetesClient client) {
        this.persistentVolumeClaim = persistentVolumeClaim;
        this.job = job;
        this.onCompleteCopyJob = onCompleteCopyJob;
        this.secret = secret;
        this.client = client;
    }

    @Override
    public void start() {
        cleanup();
        client.create(persistentVolumeClaim);
        if (secret != null) {
            client.create(secret);
        }
        client.create(job);
    }

    @Override
    public boolean waitUntilComplete() {
        var jobSucceeded = client.waitUntilJobComplete(job, STAGE_TIMEOUT_MINUTES, TimeUnit.MINUTES);
        if (!jobSucceeded) {
            return false;
        }
        client.create(onCompleteCopyJob);
        return client.waitUntilJobComplete(onCompleteCopyJob, STAGE_TIMEOUT_MINUTES, TimeUnit.MINUTES);
    }

    @Override
    public void cleanup() {
        client.deleteIfExists(secret);
        client.deleteIfExists(job);
        client.deleteIfExists(onCompleteCopyJob);
        client.deleteIfExists(persistentVolumeClaim);
    }
}
