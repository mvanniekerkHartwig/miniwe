package com.hartwig.miniwe.kubernetes;

import java.io.Closeable;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

/**
 * Wrapper around the fabric8 Kubernetes client.
 */
public class BlockingKubernetesClient implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(BlockingKubernetesClient.class);

    private final KubernetesClient client;

    public BlockingKubernetesClient() {
        this.client = new KubernetesClientBuilder().build();
    }

    public void create(PersistentVolumeClaim persistentVolumeClaim) {
        client.persistentVolumeClaims().resource(persistentVolumeClaim).create();
        LOGGER.info("Created persistent volume claim with name [{}]", persistentVolumeClaim.getMetadata().getName());
    }

    public void create(Job job) {
        client.batch().v1().jobs().resource(job).create();
        LOGGER.info("Created job with name [{}]", job.getMetadata().getName());
    }

    public boolean waitUntilJobComplete(Job job, int timeout, TimeUnit timeoutUnit) {
        var jobResource = client.batch().v1().jobs().resource(job);
        jobResource.waitUntilCondition(r -> {
            var status = r.getStatus();
            if (status == null) {
                return false;
            }
            return Objects.equals(status.getFailed(), 2) || Objects.equals(status.getSucceeded(), 1);
        }, timeout, timeoutUnit);
        return Optional.ofNullable(jobResource.get())
                .map(Job::getStatus)
                .map(JobStatus::getSucceeded)
                .map(success -> Objects.equals(1, success))
                .orElse(false);
    }

    public void deleteIfExists(Job job) {
        var jobResource = client.batch().v1().jobs().resource(job);
        if (jobResource.get() != null) {
            LOGGER.info("Deleting job with name [{}]", job.getMetadata().getName());
            jobResource.withTimeout(30, TimeUnit.SECONDS).delete();
        }
    }

    public void deleteIfExists(PersistentVolumeClaim pvc) {
        var pvcResource = client.persistentVolumeClaims().resource(pvc);
        if (pvcResource.get() != null) {
            LOGGER.info("Deleting persistent volume with name [{}]", pvc.getMetadata().getName());
            pvcResource.withTimeout(30, TimeUnit.SECONDS).delete();
        }
    }

    @Override
    public void close() {
        client.close();
    }

    @SuppressWarnings("unused")
    public KubernetesClient getFabric8Client() {
        return client;
    }
}
