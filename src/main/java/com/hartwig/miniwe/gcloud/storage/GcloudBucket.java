package com.hartwig.miniwe.gcloud.storage;

import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;

public class GcloudBucket {
    private final String bucketName;
    private final Storage storage;

    public GcloudBucket(final String bucketName, final Storage storage) {
        this.bucketName = bucketName;
        this.storage = storage;
    }

    @SuppressWarnings("unused")
    public void writeStage(String stage, InputStream content) {
        getBucket().create(stage, content);
    }

    @SuppressWarnings("unused")
    public void copyIntoStage(String stage, String fileName, byte[] content) {
        getBucket().create(stage + "/" + fileName, content);
    }

    @SuppressWarnings("unused")
    public void copyIntoStage(String stage, String gsSourcePath) {
        storage.copy(Storage.CopyRequest.newBuilder()
                .setSource(BlobId.fromGsUtilUri(gsSourcePath))
                .setTarget(BlobId.of(bucketName, stage))
                .build());
    }

    public Set<String> getCachedStages() {
        return getBucket()
                .list(Storage.BlobListOption.currentDirectory())
                .streamAll()
                .filter(blob -> blob.getName().endsWith("/"))
                .map(blob -> blob.getName().substring(0, blob.getName().length() - 1))
                .collect(Collectors.toUnmodifiableSet());
    }

    private Bucket getBucket() {
        return storage.get(bucketName);
    }
}
