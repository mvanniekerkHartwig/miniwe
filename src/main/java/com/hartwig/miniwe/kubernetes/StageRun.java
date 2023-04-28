package com.hartwig.miniwe.kubernetes;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;

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
        var jobResource = client.batch().v1().jobs().inNamespace(namespace).resource(job);
        jobResource.create();
        jobResource.waitUntilReady(1, TimeUnit.MINUTES);
        LOGGER.info("Created job with name {}", job.getMetadata().getName());
    }

    public void waitUntilComplete() {
        var podList = client.pods().inNamespace(namespace).withLabel("job-name", job.getMetadata().getName()).list();
        var pod = podList.getItems().get(0);
        var podResource = client.pods().resource(pod);
        podResource.waitUntilCondition(r -> r.getStatus().getPhase().equals("Terminating"), 15, TimeUnit.MINUTES);
    }

    @Override
    public void close() {
        LOGGER.info("Cleaning up run...");
        var pvcName = persistentVolumeClaim.getMetadata().getName();
        client.persistentVolumeClaims().inNamespace(namespace).resource(persistentVolumeClaim).delete();
        LOGGER.info("Deleted Persistent volume claim with name {}", pvcName);
        var jobName = job.getMetadata().getName();
        client.batch().v1().jobs().inNamespace(namespace).resource(job).delete();
        LOGGER.info("Deleted job with name {}", jobName);
    }
}
