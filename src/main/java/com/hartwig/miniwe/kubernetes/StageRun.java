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
    private final String namespace;

    private final KubernetesClient client;

    public StageRun(final PersistentVolumeClaim persistentVolumeClaim, final Job job, final String namespace,
            final KubernetesClient client) {
        this.persistentVolumeClaim = persistentVolumeClaim;
        this.job = job;
        this.namespace = namespace;
        this.client = client;
    }

    public void start() {
        client.persistentVolumeClaims().resource(persistentVolumeClaim).create();
        LOGGER.info("Created Persistent volume claim with name {}", persistentVolumeClaim.getMetadata().getName());
        client.batch().v1().jobs().inNamespace(namespace).resource(job).create();
        LOGGER.info("Created job with name {}", job.getMetadata().getName());
    }

    public boolean waitUntilComplete() {
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
        LOGGER.info("Cleaning up run...");
        var pvcName = persistentVolumeClaim.getMetadata().getName();
        client.persistentVolumeClaims().inNamespace(namespace).resource(persistentVolumeClaim).delete();
        LOGGER.info("Deleted Persistent volume claim with name {}", pvcName);
        var jobName = job.getMetadata().getName();
        client.batch().v1().jobs().inNamespace(namespace).resource(job).delete();
        LOGGER.info("Deleted job with name {}", jobName);
    }
}
