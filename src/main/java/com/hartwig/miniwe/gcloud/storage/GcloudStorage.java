package com.hartwig.miniwe.gcloud.storage;

import java.util.HashMap;
import java.util.Map;

import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.hartwig.miniwe.kubernetes.KubernetesUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcloudStorage {
    private static final Logger LOGGER = LoggerFactory.getLogger(GcloudStorage.class);

    private final Storage storage;
    private final String gcpRegion;

    private final Map<String, GcloudBucket> bucketByRunName = new HashMap<>();

    public GcloudStorage(final Storage storage, final String gcpRegion) {
        this.storage = storage;
        this.gcpRegion = gcpRegion;
    }

    public GcloudBucket findOrCreateBucket(String runName) {
        if (bucketByRunName.containsKey(runName)) {
            return bucketByRunName.get(runName);
        }
        var bucketName = KubernetesUtil.toValidRFC1123Label("run", runName);
        var bucket = storage.get(bucketName);
        if (bucket != null) {
            LOGGER.warn("Bucket [{}] already exists. Reusing it.", bucketName);
        } else {
            var bucketInfo = BucketInfo.newBuilder(bucketName).setLocation(gcpRegion).build();
            bucket = storage.create(bucketInfo);
            LOGGER.info("Created run bucket [{}] in project [{}]", bucketName, storage.getOptions().getProjectId());
        }
        var gcloudBucket = new GcloudBucket(bucket);
        bucketByRunName.put(runName, gcloudBucket);
        return gcloudBucket;
    }

    public void deleteBucket(String runName) {
        var bucket = bucketByRunName.remove(runName);
        if (bucket == null) {
            LOGGER.warn("Could not find bucket to delete for runName '{}'.", runName);
            return;
        }
        bucket.cleanup();
        LOGGER.info("Deleted bucket for run '{}'", runName);
    }
}
