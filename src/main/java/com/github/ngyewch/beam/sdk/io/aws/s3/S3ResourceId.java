package com.github.ngyewch.beam.sdk.io.aws.s3;

import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.commons.io.FilenameUtils;

import javax.annotation.Nullable;

public class S3ResourceId implements ResourceId {

    private final String bucketName;
    private final String key;
    private final boolean directory;

    public S3ResourceId(String bucketName, String key, boolean directory) {
        super();

        this.bucketName = bucketName;
        this.key = key;
        this.directory = directory;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getKey() {
        return key;
    }

    @Override
    public ResourceId resolve(String other, ResolveOptions resolveOptions) {
        if (!isDirectory()) {
            throw new IllegalStateException();
        }
        String newKey;
        if (key.length() > 0) {
            newKey = key + "/" + other;
        } else {
            newKey = other;
        }
        ResourceId resourceId;
        if (resolveOptions == ResolveOptions.StandardResolveOptions.RESOLVE_FILE) {
            resourceId = new S3ResourceId(bucketName, newKey, false);
        } else if (resolveOptions == ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY) {
            resourceId = new S3ResourceId(bucketName, newKey, true);
        } else {
            throw new IllegalArgumentException("Unsupported resolve option: " + resolveOptions);
        }
        return resourceId;
    }

    @Override
    public ResourceId getCurrentDirectory() {
        final String path = FilenameUtils.getPath(key);
        if (path == null) {
            throw new IllegalArgumentException();
        } else if (path.length() == 0) {
            return new S3ResourceId(bucketName, path, true);
        } else {
            return new S3ResourceId(bucketName, path.substring(0, path.length() - 1), true);
        }
    }

    @Override
    public String getScheme() {
        return "s3";
    }

    @Nullable
    @Override
    public String getFilename() {
        return FilenameUtils.getName(key);
    }

    @Override
    public boolean isDirectory() {
        return directory;
    }

    public String toString() {
        if (directory) {
            return String.format("%s://%s/%s/", getScheme(), bucketName, key);
        } else {
            return String.format("%s://%s/%s", getScheme(), bucketName, key);
        }
    }
}
