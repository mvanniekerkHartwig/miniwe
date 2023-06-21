package com.hartwig.miniwe.gcloud.storage;

import java.util.List;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.hartwig.miniwe.kubernetes.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;

public class GcloudStorageProvider implements StorageProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(GcloudStorageProvider.class);
    private final Bucket bucket;

    private GcloudStorageProvider(final Bucket bucket) {
        this.bucket = bucket;
    }

    public static GcloudStorageProvider create(Storage storage, String gcpRegion, String bucketName) {
        var bucketInfo = BucketInfo.newBuilder(bucketName).setLocation(gcpRegion).build();
        var bucket = storage.create(bucketInfo);
        LOGGER.info("Created run bucket [{}] in project [{}]", bucketName, storage.getOptions().getProjectId());
        return new GcloudStorageProvider(bucket);
    }

    public void cleanup() {
        bucket.delete();
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
        return new ContainerBuilder().withName(outputStage + "-output")
                .withImage("eu.gcr.io/hmf-build/google/cloud-sdk:425.0.0")
                .withCommand("sh",
                        "-c",
                        String.format("until [ -f /out/ready ]; do sleep 1; done && gsutil rsync /out gs://%s/%s", bucket.getName(), outputStage))
                .withVolumeMounts(new VolumeMountBuilder().withName(volumeName).withMountPath("/out").build())
                .build();
    }
}
