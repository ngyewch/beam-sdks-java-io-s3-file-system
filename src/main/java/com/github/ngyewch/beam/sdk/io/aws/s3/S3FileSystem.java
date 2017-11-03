package com.github.ngyewch.beam.sdk.io.aws.s3;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.AntPathMatcher;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class S3FileSystem extends FileSystem<S3ResourceId> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final S3FileSystemOptions options;
    private final AmazonS3 amazonS3;

    private final AntPathMatcher antPathMatcher = new AntPathMatcher();

    private final Map<String, CacheEntry> cacheMap = new HashMap<>();

    public S3FileSystem(S3FileSystemOptions options) {
        super();

        this.options = options;

        AWSCredentialsProvider awsCredentialsProvider;
        if (options.getAwsProfileId() != null) {
            awsCredentialsProvider = new ProfileCredentialsProvider(options.getAwsProfileId());
        } else if ((options.getAwsAccessKeyId() != null) && (options.getAwsSecretAccessKey() != null)) {
            awsCredentialsProvider = new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(options.getAwsAccessKeyId(), options.getAwsSecretAccessKey()));
        } else {
            awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
        }
        final AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3ClientBuilder.standard()
                .withCredentials(awsCredentialsProvider);
        if (options.getAwsRegion() != null) {
            amazonS3ClientBuilder.withRegion(options.getAwsRegion());
        }
        amazonS3 = amazonS3ClientBuilder.build();

        if (isCacheEnabled()) {
            final Thread housekeepingThread = new Thread(new HousekeepingWorker());
            housekeepingThread.setDaemon(true);
            housekeepingThread.start();
        }
    }

    private boolean isCacheEnabled() {
        return ((options.getAwsS3MaxCacheAgeInSeconds() != null) && (options.getAwsS3MaxCacheAgeInSeconds() > 0));
    }

    @Override
    protected List<MatchResult> match(List<String> specs) throws IOException {
        final List<MatchResult> matchResults = new ArrayList<>();
        for (final String spec : specs) {
            try {
                final URI uri = new URI(spec);
                if (uri.getPort() == -1) {
                    final String bucketName = uri.getHost();
                    final String path = uri.getPath().substring(1);
                    final int p = path.indexOf('*');
                    final List<MatchResult.Metadata> metadataList = new ArrayList<>();
                    if (p >= 0) {
                        final String pathPrefix = path.substring(0, p);
                        final String pattern = path.substring(p);
                        final ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request()
                                .withBucketName(bucketName)
                                .withPrefix(pathPrefix);
                        while (true) {
                            final ListObjectsV2Result listObjectsV2Result = amazonS3.listObjectsV2(
                                    listObjectsV2Request);
                            for (final S3ObjectSummary objectSummary : listObjectsV2Result.getObjectSummaries()) {
                                final String variablePart = objectSummary.getKey().substring(pathPrefix.length());
                                if (antPathMatcher.match(pattern, variablePart)) {
                                    metadataList.add(MatchResult.Metadata.builder()
                                            .setIsReadSeekEfficient(true)
                                            .setResourceId(new S3ResourceId(bucketName, objectSummary.getKey(), false))
                                            .setSizeBytes(objectSummary.getSize())
                                            .build());
                                }
                            }
                            if (listObjectsV2Result.getContinuationToken() == null) {
                                break;
                            } else {
                                listObjectsV2Request.setBucketName(listObjectsV2Result.getContinuationToken());
                            }
                        }
                        if (metadataList.isEmpty()) {
                            matchResults.add(MatchResult.create(MatchResult.Status.NOT_FOUND,
                                    new FileNotFoundException(spec)));
                        } else {
                            matchResults.add(MatchResult.create(MatchResult.Status.OK,
                                    Collections.unmodifiableList(metadataList)));
                        }
                    } else {
                        final GetObjectMetadataRequest getObjectMetadataRequest = new GetObjectMetadataRequest(
                                bucketName, path);
                        try {
                            final ObjectMetadata objectMetadata = amazonS3.getObjectMetadata(getObjectMetadataRequest);
                            matchResults.add(MatchResult.create(MatchResult.Status.OK,
                                    ImmutableList.of(MatchResult.Metadata.builder()
                                            .setIsReadSeekEfficient(true)
                                            .setResourceId(new S3ResourceId(bucketName, path, false))
                                            .setSizeBytes(objectMetadata.getContentLength())
                                            .build())));
                        } catch (AmazonS3Exception e) {
                            if (e.getStatusCode() == 404) {
                                matchResults.add(MatchResult.create(MatchResult.Status.NOT_FOUND,
                                        new FileNotFoundException(spec)));
                            }
                        }
                    }
                } else {
                    throw new IllegalArgumentException(spec);
                }
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(spec, e);
            }
        }
        return Collections.unmodifiableList(matchResults);
    }

    @Override
    protected WritableByteChannel create(S3ResourceId resourceId, CreateOptions createOptions) throws IOException {
        final File tempFile = File.createTempFile(FilenameUtils.getBaseName(resourceId.getFilename()),
                "." + FilenameUtils.getExtension(resourceId.getFilename()));
        tempFile.deleteOnExit();
        return new S3WritableByteChannel(resourceId, tempFile, createOptions.mimeType());
    }

    @Override
    protected ReadableByteChannel open(S3ResourceId resourceId) throws IOException {
        File file = null;
        if (isCacheEnabled()) {
            final CacheEntry cacheEntry = cacheMap.get(resourceId.toString());
            if (cacheEntry != null) {
                if (cacheEntry.isValid(options.getAwsS3MaxCacheAgeInSeconds())) {
                    if (cacheEntry.getFile().isFile()) {
                        file = cacheEntry.getFile();
                        cacheEntry.touch();
                    } else {
                        cacheMap.remove(resourceId.toString());
                        log.debug("Cache entry for {} removed, local file does not exist", resourceId);
                    }
                } else {
                    cacheEntry.getFile().delete();
                    cacheMap.remove(resourceId.toString());
                    log.debug("Cache entry for {} removed, exceeded max cache age", resourceId);
                }
            }
        }
        if (file == null) {
            final GetObjectRequest getObjectRequest = new GetObjectRequest(resourceId.getBucketName(),
                    resourceId.getKey());
            file = File.createTempFile(FilenameUtils.getBaseName(resourceId.getKey()),
                    "." + FilenameUtils.getExtension(resourceId.getKey()));
            file.deleteOnExit();

            log.debug("Downloading {}", resourceId);
            final ObjectMetadata objectMetadata = amazonS3.getObject(getObjectRequest, file);
            log.debug("Downloaded {}", resourceId);

            final CacheEntry cacheEntry = new CacheEntry(file);
            cacheMap.put(resourceId.toString(), cacheEntry);
        }
        return new S3ReadableByteChannel(resourceId, file);
    }

    @Override
    protected void copy(List<S3ResourceId> srcResourceIds, List<S3ResourceId> destResourceIds) throws IOException {
        log.debug("copy({}, {})", srcResourceIds, destResourceIds);
        for (int i = 0; i < srcResourceIds.size(); i++) {
            final S3ResourceId srcResourceId = srcResourceIds.get(i);
            final S3ResourceId destResourceId = destResourceIds.get(i);
            final CopyObjectRequest copyObjectRequest = new CopyObjectRequest(srcResourceId.getBucketName(),
                    srcResourceId.getKey(), destResourceId.getBucketName(), destResourceId.getKey());
            final CopyObjectResult copyObjectResult = amazonS3.copyObject(copyObjectRequest);
        }
    }

    @Override
    protected void rename(List<S3ResourceId> srcResourceIds, List<S3ResourceId> destResourceIds) throws IOException {
        log.debug("rename({}, {})", srcResourceIds, destResourceIds);
        copy(srcResourceIds, destResourceIds);
        delete(srcResourceIds);
    }

    @Override
    protected void delete(Collection<S3ResourceId> resourceIds) throws IOException {
        log.debug("delete({})", resourceIds);
        for (final S3ResourceId resourceId : resourceIds) {
            final DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(resourceId.getBucketName(),
                    resourceId.getKey());
            amazonS3.deleteObject(deleteObjectRequest);
        }
    }

    @Override
    protected S3ResourceId matchNewResource(String singleResourceSpec, boolean isDirectory) {
        try {
            final URI uri = new URI(singleResourceSpec);
            if (uri.getPort() != -1) {
                throw new IllegalArgumentException(singleResourceSpec);
            }
            final String bucketName = uri.getHost();
            final String key = uri.getPath().substring(1);
            S3ResourceId resourceId;
            if (isDirectory && key.endsWith("/")) {
                resourceId = new S3ResourceId(bucketName, key.substring(0, key.length() - 1), isDirectory);
            } else {
                resourceId = new S3ResourceId(bucketName, key, isDirectory);
            }
            return resourceId;
        } catch (URISyntaxException e) {
            log.error("Exception", e);
            throw new IllegalArgumentException(singleResourceSpec, e);
        }
    }

    @Override
    protected String getScheme() {
        return "s3";
    }

    private static class CacheEntry {

        private final File file;
        private long lastAccessTime;

        public CacheEntry(File file) {
            this(file, System.currentTimeMillis());
        }

        public CacheEntry(File file, long lastAccessTime) {
            super();

            this.file = file;
            this.lastAccessTime = lastAccessTime;
        }

        public File getFile() {
            return file;
        }

        public void touch() {
            touch(System.currentTimeMillis());
        }

        public void touch(long lastAccessTime) {
            this.lastAccessTime = lastAccessTime;
        }

        public boolean isValid(int maxAgeInSeconds) {
            return isValid(System.currentTimeMillis(), maxAgeInSeconds);
        }

        public boolean isValid(long currentTime, int maxAgeInSeconds) {
            final long ageInMillis = currentTime - lastAccessTime;
            final long ageInSeconds = ageInMillis / 1000;
            return (ageInSeconds <= maxAgeInSeconds);
        }
    }

    private class S3ReadableByteChannel implements ReadableByteChannel, SeekableByteChannel {

        private final S3ResourceId resourceId;
        private final File file;
        private FileChannel delegate;

        public S3ReadableByteChannel(S3ResourceId resourceId, File file) throws IOException {
            super();

            this.resourceId = resourceId;
            this.file = file;

            this.delegate = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        }

        @Override
        public int read(ByteBuffer byteBuffer) throws IOException {
            return delegate.read(byteBuffer);
        }

        @Override
        public int write(ByteBuffer byteBuffer) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long position() throws IOException {
            return delegate.position();
        }

        @Override
        public SeekableByteChannel position(long newPosition) throws IOException {
            return delegate.position(newPosition);
        }

        @Override
        public long size() throws IOException {
            return delegate.size();
        }

        @Override
        public SeekableByteChannel truncate(long size) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isOpen() {
            return delegate.isOpen();
        }

        @Override
        public void close() throws IOException {
            delegate.close();

            if (!isCacheEnabled()) {
                file.delete();
                log.debug("Deleted local copy of {}", resourceId);
            }
        }
    }

    private class S3WritableByteChannel implements WritableByteChannel {

        private final S3ResourceId resourceId;
        private final File file;
        private final String mimeType;

        private final FileChannel delegate;

        public S3WritableByteChannel(S3ResourceId resourceId, File file, String mimeType) throws IOException {
            super();

            this.resourceId = resourceId;
            this.file = file;
            this.mimeType = mimeType;

            this.delegate = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        }

        @Override
        public int write(ByteBuffer byteBuffer) throws IOException {
            return delegate.write(byteBuffer);
        }

        @Override
        public boolean isOpen() {
            return delegate.isOpen();
        }

        @Override
        public void close() throws IOException {
            delegate.close();

            final ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentLength(file.length());
            objectMetadata.setContentType(mimeType);

            final PutObjectRequest putObjectRequest = new PutObjectRequest(resourceId.getBucketName(),
                    resourceId.getKey(), file).withMetadata(objectMetadata);
            log.debug("Uploading {}", resourceId);
            final PutObjectResult putObjectResult = amazonS3.putObject(putObjectRequest);
            log.debug("Uploaded {}", resourceId);
        }
    }

    private class HousekeepingWorker implements Runnable {

        @Override
        public void run() {
            while (true) {
                log.debug("Housekeeping started");
                try {
                    final List<String> idsToRemove = new ArrayList<>();
                    for (final Map.Entry<String, CacheEntry> entry : cacheMap.entrySet()) {
                        if (!entry.getValue().isValid(options.getAwsS3MaxCacheAgeInSeconds())) {
                            idsToRemove.add(entry.getKey());
                        }
                    }
                    for (final String id : idsToRemove) {
                        final CacheEntry cacheEntry = cacheMap.get(id);
                        if (cacheEntry != null) {
                            cacheEntry.getFile().delete();
                            cacheMap.remove(id);
                            log.debug("Housekeeping removed {} from cache", id);
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception", e);
                }
                log.debug("Housekeeping finished");
                try {
                    Thread.sleep(10 * 60 * 1000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }
}
