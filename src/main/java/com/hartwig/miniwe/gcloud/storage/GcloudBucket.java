package com.hartwig.miniwe.gcloud.storage;

import java.io.InputStream;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;

public class GcloudBucket {
    private final String bucketName;
    private final Storage storage;

    public GcloudBucket(final String bucketName, final Storage storage) {
        this.bucketName = bucketName;
        this.storage = storage;
    }

    @SuppressWarnings("unused")
    public void copyIntoStage(String stage, String filename, byte[] content) {
        getBucket().create(stage + "/" + filename, content);
    }

    @SuppressWarnings("unused")
    public void copyIntoStage(String stage, String filename, InputStream content) {
        getBucket().create(stage + "/" + filename, content);
    }

    @SuppressWarnings("unused")
    public void copyIntoStage(String stage, String filename, String gsSourcePath) {
        storage.copy(Storage.CopyRequest.newBuilder()
                .setSource(BlobId.fromGsUtilUri(gsSourcePath))
                .setTarget(getBlobId(stage, filename))
                .build());
    }

    public BlobId getBlobId(final String stage, final String filename) {
        return BlobId.of(bucketName, stage + "/" + filename);
    }

    @SuppressWarnings("unused")
    public void copyOutOfStage(String stage, String filename, String gsTargetPath) {
        storage.copy(Storage.CopyRequest.newBuilder()
                .setSource(getBlobId(stage, filename))
                .setTarget(BlobId.fromGsUtilUri(gsTargetPath))
                .build());
    }

    public Set<String> getCachedStages() {
        return getBucket().list(Storage.BlobListOption.currentDirectory())
                .streamAll()
                .filter(blob -> blob.getName().endsWith("/"))
                .map(blob -> blob.getName().substring(0, blob.getName().length() - 1))
                .collect(Collectors.toUnmodifiableSet());
    }

    private Bucket getBucket() {
        return storage.get(bucketName);
    }
}
