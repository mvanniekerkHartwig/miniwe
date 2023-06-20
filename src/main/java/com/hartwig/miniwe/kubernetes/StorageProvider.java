package com.hartwig.miniwe.kubernetes;

import io.fabric8.kubernetes.api.model.Container;

public interface StorageProvider {
    Container initStorageContainer(String runName, String inputStage, String volumeName);

    Container exitStorageContainer(String runName, String outputStage, String volumeName);
}
