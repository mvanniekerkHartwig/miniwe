package com.hartwig.miniwe.kubernetes;

import java.util.List;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;

public class GcloudStorageProvider implements StorageProvider {
    @Override
    public Container initStorageContainer(final String bucketName, final String stage, final String volumeName) {
        var command = String.format("gsutil -m rsync -r gs://%s/%s /in", bucketName, stage);
        return new ContainerBuilder().withName(stage + "-input")
                .withImage("eu.gcr.io/hmf-build/google/cloud-sdk:425.0.0")
                .withCommand(List.of("sh", "-c", command))
                .withVolumeMounts(new VolumeMountBuilder().withName(volumeName).withMountPath("/in").withSubPath(stage).build())
                .build();
    }

    @Override
    public Container exitStorageContainer(final String bucketName, final String stage, final String volumeName) {
        return new ContainerBuilder().withName(stage + "-copier")
                .withImage("eu.gcr.io/hmf-build/google/cloud-sdk:425.0.0")
                .withCommand("sh", "-c", String.format("gsutil -m rsync -r /out gs://%s/%s", bucketName, stage))
                .withVolumeMounts(new VolumeMountBuilder().withName(volumeName).withMountPath("/out").withSubPath(stage).build())
                .build();
    }
}
