package com.hartwig.miniwe.kubernetes;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.batch.v1.Job;

class NoOutputStageRun implements StageRun {
    private static final int STAGE_TIMEOUT_MINUTES = 15;
    private final Job job;

    private final BlockingKubernetesClient client;

    NoOutputStageRun(Job job, BlockingKubernetesClient client) {
        this.job = job;
        this.client = client;
    }

    @Override
    public void start() {
        cleanup();
        client.create(job);
    }

    @Override
    public boolean waitUntilComplete() {
        return client.waitUntilJobComplete(job, STAGE_TIMEOUT_MINUTES, TimeUnit.MINUTES);
    }

    @Override
    public void cleanup() {
        client.deleteIfExists(job);
    }
}
