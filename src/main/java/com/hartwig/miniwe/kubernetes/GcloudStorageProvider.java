package com.hartwig.miniwe.kubernetes;

import java.util.List;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;

public class GcloudStorageProvider implements StorageProvider {
    @Override
    public Container initStorageContainer(final String bucketName, final String inputStage, final String volumeName) {
        return new ContainerBuilder().withName(inputStage + "-input")
                .withImage("eu.gcr.io/hmf-build/google/cloud-sdk:425.0.0")
                .withCommand(List.of("sh", "-c", String.format("gsutil rsync gs://%s/%s /in", bucketName, inputStage)))
                .withVolumeMounts(new VolumeMountBuilder().withName(volumeName).withMountPath("/in").build())
                .build();
    }

    @Override
    public Container exitStorageContainer(final String bucketName, final String outputStage, final String volumeName) {
        return new ContainerBuilder().withName(outputStage + "-copier")
                .withImage("eu.gcr.io/hmf-build/google/cloud-sdk:425.0.0")
                .withCommand("sh", "-c", String.format("gsutil rsync /out gs://%s/%s", bucketName, outputStage))
                .withVolumeMounts(new VolumeMountBuilder().withName(volumeName).withMountPath("/out").build())
                .build();
    }
}
