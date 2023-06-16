package com.hartwig.miniwe.gcloud.storage;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.miniwe.kubernetes.StorageProvider;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;

public class GcloudBucket implements StorageProvider {
    private final Bucket bucket;

    GcloudBucket(final Bucket bucket) {
        this.bucket = bucket;
    }

    public void cleanup() {
        bucket.delete();
    }

    public Set<String> getCachedStages() {
        return bucket.list(Storage.BlobListOption.currentDirectory())
                .streamAll()
                .filter(blob -> blob.getName().endsWith("/"))
                .map(blob -> blob.getName().substring(0, blob.getName().length() - 1))
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public Container initStorageContainer(String inputStage, String volumeName) {
        return new ContainerBuilder().withName(inputStage + "-input")
                .withImage("eu.gcr.io/hmf-build/google/cloud-sdk:425.0.0")
                .withCommand(List.of("sh", "-c", String.format("gsutil rsync gs://%s/%s /in", bucket.getName(), inputStage)))
                .withVolumeMounts(new VolumeMountBuilder().withName(volumeName).withMountPath("/in").build())
                .build();
    }

    @Override
    public Container exitStorageContainer(String outputStage, String volumeName) {
        return new ContainerBuilder().withName(outputStage + "-copier")
                .withImage("eu.gcr.io/hmf-build/google/cloud-sdk:425.0.0")
                .withCommand("sh", "-c", String.format("gsutil rsync /out gs://%s/%s", bucket.getName(), outputStage))
                .withVolumeMounts(new VolumeMountBuilder().withName(volumeName).withMountPath("/out").build())
                .build();
    }
}
