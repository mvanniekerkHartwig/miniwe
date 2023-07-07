package com.hartwig.miniwe.kubernetes;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.batch.v1.Job;

class OutputStageRun implements StageRun {
    private static final int STAGE_TIMEOUT_MINUTES = 15;

    private final List<PersistentVolumeClaim> pvcs;
    private final List<Job> jobs;
    private final Secret secret;

    private final BlockingKubernetesClient client;

    OutputStageRun(PersistentVolumeClaim inputPvc, PersistentVolumeClaim outputPvc, Job stageJob, Job inputJob, Job onCompleteCopyJob,
            Secret secret, BlockingKubernetesClient client) {
        this.pvcs = Stream.of(inputPvc, outputPvc).filter(Objects::nonNull).collect(Collectors.toList());
        this.jobs = Stream.of(inputJob, stageJob, onCompleteCopyJob).filter(Objects::nonNull).collect(Collectors.toList());
        this.secret = secret;
        this.client = client;
    }

    @Override
    public boolean start() {
        cleanup();
        for (final PersistentVolumeClaim pvc : pvcs) {
            client.create(pvc);
        }
        if (secret != null) {
            client.create(secret);
        }
        return runInOrder(jobs);
    }

    private boolean runInOrder(List<Job> jobs) {
        for (final Job job : jobs) {
            client.create(job);
            var jobSucceeded = client.waitUntilJobComplete(job, STAGE_TIMEOUT_MINUTES, TimeUnit.MINUTES);
            if (!jobSucceeded) {
                return false;
            }
            client.deleteIfExists(job);
        }
        return true;
    }

    @Override
    public void cleanup() {
        for (Job job : jobs) {
            client.deleteIfExists(job);
        }
        for (final PersistentVolumeClaim pvc : pvcs) {
            client.deleteIfExists(pvc);
        }
        if (secret != null) {
            client.deleteIfExists(secret);
        }
    }
}
