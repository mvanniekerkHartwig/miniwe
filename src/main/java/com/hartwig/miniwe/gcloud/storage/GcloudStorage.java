package com.hartwig.miniwe.gcloud.storage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.hartwig.miniwe.kubernetes.StorageProvider;
import com.hartwig.miniwe.workflow.WorkflowUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Container;

public class GcloudStorage implements StorageProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(GcloudStorage.class);

    private final Storage storage;
    private final String gcpRegion;

    private final ConcurrentMap<String, GcloudBucket> bucketByRunName = new ConcurrentHashMap<>();

    public GcloudStorage(Storage storage, String gcpRegion) {
        this.storage = storage;
        this.gcpRegion = gcpRegion;
    }

    public GcloudBucket findOrCreateBucket(String runName) {
        if (bucketByRunName.containsKey(runName)) {
            return bucketByRunName.get(runName);
        }
        var bucketName = WorkflowUtil.getBucketName(runName);
        var bucket = storage.get(bucketName);
        if (bucket != null) {
            LOGGER.warn("[{}] Bucket already exists, reusing it.", bucketName);
        } else {
            var bucketInfo = BucketInfo.newBuilder(bucketName).setLocation(gcpRegion).build();
            bucket = storage.create(bucketInfo);
            LOGGER.info("[{}] Created run bucket in project [{}]", bucketName, storage.getOptions().getProjectId());
        }
        var gcloudBucket = new GcloudBucket(bucket);
        bucketByRunName.put(runName, gcloudBucket);
        return gcloudBucket;
    }

    @Override
    public Container initStorageContainer(String runName, String inputStage, String volumeName) {
        return findOrCreateBucket(runName).initStorageContainer(inputStage, volumeName);
    }

    @Override
    public Container exitStorageContainer(String runName, String outputStage, String volumeName) {
        return findOrCreateBucket(runName).exitStorageContainer(outputStage, volumeName);
    }
}
