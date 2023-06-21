package com.hartwig.miniwe.gcloud.storage;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.miniwe.kubernetes.StorageProvider;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;

public class GcloudBucket {
    private final Bucket bucket;

    public GcloudBucket(Bucket bucket) {
        this.bucket = bucket;
    }

    public Set<String> getCachedStages() {
        return bucket.list(Storage.BlobListOption.currentDirectory())
                .streamAll()
                .filter(blob -> blob.getName().endsWith("/"))
                .map(blob -> blob.getName().substring(0, blob.getName().length() - 1))
                .collect(Collectors.toUnmodifiableSet());
    }

    public Container initStorageContainer(String inputStage, String volumeName) {
        var bucketName = Objects.requireNonNull(bucket.getName());
        return new ContainerBuilder().withName(inputStage + "-input")
                .withImage("eu.gcr.io/hmf-build/google/cloud-sdk:425.0.0")
                .withCommand(List.of("sh", "-c", String.format("gsutil rsync gs://%s/%s /in", bucketName, inputStage)))
                .withVolumeMounts(new VolumeMountBuilder().withName(volumeName).withMountPath("/in").build())
                .build();
    }

    public Container exitStorageContainer(String outputStage, String volumeName) {
        var bucketName = Objects.requireNonNull(bucket.getName());
        return new ContainerBuilder().withName(outputStage + "-copier")
                .withImage("eu.gcr.io/hmf-build/google/cloud-sdk:425.0.0")
                .withCommand("sh", "-c", String.format("gsutil rsync /out gs://%s/%s", bucketName, outputStage))
                .withVolumeMounts(new VolumeMountBuilder().withName(volumeName).withMountPath("/out").build())
                .build();
    }
}
