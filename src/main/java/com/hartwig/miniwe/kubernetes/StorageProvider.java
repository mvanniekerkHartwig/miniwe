package com.hartwig.miniwe.kubernetes;

import io.fabric8.kubernetes.api.model.Container;

public interface StorageProvider {
    Container initStorageContainer(String bucketName, String inputStage, String volumeName);

    Container exitStorageContainer(String bucketName, String outputStage, String volumeName);
}
