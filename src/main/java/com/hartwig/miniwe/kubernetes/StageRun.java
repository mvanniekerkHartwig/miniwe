package com.hartwig.miniwe.kubernetes;

import java.io.Closeable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobCondition;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ScalableResource;

public class StageRun implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(StageRun.class);

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
            if (status != null) {
                if (status.getFailed() != null && status.getFailed() > 1) {
                    return true;
                } else if (status.getSucceeded() != null && status.getSucceeded() > 0) {
                    return true;
                }
            }
            return false;
        }, 15, TimeUnit.MINUTES);
        return Optional.ofNullable(jobResource.get())
                .map(Job::getStatus)
                .map(JobStatus::getSucceeded)
                .map(success -> Objects.equals(1, success))
                .orElse(false);
    }

    @Override
    public void close() {
        LOGGER.info("Cleaning up run...");
        var jobName = job.getMetadata().getName();
        client.batch().v1().jobs().inNamespace(namespace).resource(job).delete();
        LOGGER.info("Deleted job with name {}", jobName);
    }
}
