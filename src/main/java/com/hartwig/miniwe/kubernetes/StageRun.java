package com.hartwig.miniwe.kubernetes;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import io.fabric8.kubernetes.client.KubernetesClient;

public class StageRun {
    private static final Logger LOGGER = LoggerFactory.getLogger(StageRun.class);
    private static final int STAGE_TIMEOUT_MINUTES = 15;

    private final PersistentVolumeClaim persistentVolumeClaim;
    private final Job job;
    private final Job onCompleteCopyJob;
    private final String namespace;

    private final KubernetesClient client;

    public StageRun(final PersistentVolumeClaim persistentVolumeClaim, final Job job, final Job onCompleteCopyJob, final String namespace,
            final KubernetesClient client) {
        this.persistentVolumeClaim = persistentVolumeClaim;
        this.job = job;
        this.onCompleteCopyJob = onCompleteCopyJob;
        this.namespace = namespace;
        this.client = client;
    }

    public void start() {
        cleanup();
        client.persistentVolumeClaims().resource(persistentVolumeClaim).create();
        LOGGER.info("Created persistent volume claim with name [{}]", persistentVolumeClaim.getMetadata().getName());

        client.batch().v1().jobs().inNamespace(namespace).resource(job).create();
        LOGGER.info("Created job with name [{}]", job.getMetadata().getName());
    }

    public boolean waitUntilComplete() {
        var jobSucceeded = waitUntilJobComplete(job);
        if (!jobSucceeded) {
            return false;
        }
        client.batch().v1().jobs().inNamespace(namespace).resource(onCompleteCopyJob).create();
        LOGGER.info("Created on complete copy job with name [{}]", onCompleteCopyJob.getMetadata().getName());
        return waitUntilJobComplete(onCompleteCopyJob);
    }

    private boolean waitUntilJobComplete(final Job job) {
        var jobResource = client.batch().v1().jobs().inNamespace(namespace).resource(job);
        jobResource.waitUntilCondition(r -> {
            var status = r.getStatus();
            if (status == null) {
                return false;
            }
            return Objects.equals(status.getFailed(), 2) || Objects.equals(status.getSucceeded(), 1);
        }, STAGE_TIMEOUT_MINUTES, TimeUnit.MINUTES);
        return Optional.ofNullable(jobResource.get())
                .map(Job::getStatus)
                .map(JobStatus::getSucceeded)
                .map(success -> Objects.equals(1, success))
                .orElse(false);
    }

    public void cleanup() {
        LOGGER.info("Cleaning up resources for job [{}]...", job.getMetadata().getName());
        deleteIfExists(job);
        deleteIfExists(onCompleteCopyJob);
        deleteIfExists(persistentVolumeClaim);
    }

    private void deleteIfExists(final Job job) {
        var jobResource = client.batch().v1().jobs().inNamespace(namespace).resource(job);
        if (jobResource.get() != null) {
            LOGGER.info("Deleting job with name [{}]", job.getMetadata().getName());
            jobResource.withTimeout(30, TimeUnit.SECONDS).delete();
        }
    }

    private void deleteIfExists(final PersistentVolumeClaim pvc) {
        var pvcResource = client.persistentVolumeClaims().resource(pvc);
        if (pvcResource.get() != null) {
            LOGGER.info("Deleting persistent volume with name [{}]", pvc.getMetadata().getName());
            pvcResource.withTimeout(30, TimeUnit.SECONDS).delete();
        }
    }
}
