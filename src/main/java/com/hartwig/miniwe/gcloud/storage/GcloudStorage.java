package com.hartwig.miniwe.gcloud.storage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.hartwig.miniwe.kubernetes.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Container;

public class GcloudStorage {
    private static final Logger LOGGER = LoggerFactory.getLogger(GcloudStorage.class);

    private final Storage storage;
    private final String gcpRegion;

    private final ConcurrentMap<String, GcloudBucket> bucketByName = new ConcurrentHashMap<>();

    public GcloudStorage(Storage storage, String gcpRegion) {
        this.storage = storage;
        this.gcpRegion = gcpRegion;
    }

    public GcloudBucket findOrCreateBucket(String bucketName) {
        return bucketByName.computeIfAbsent(bucketName, name -> {
            if (storage.get(bucketName) != null) {
                LOGGER.info("[{}] Bucket already exists.", bucketName);
            } else {
                var bucketInfo = BucketInfo.newBuilder(bucketName).setLocation(gcpRegion).build();
                storage.create(bucketInfo);
                LOGGER.info("[{}] Created run bucket in project [{}]", bucketName, storage.getOptions().getProjectId());
            }
            return new GcloudBucket(bucketName, storage);
        });
    }
}
