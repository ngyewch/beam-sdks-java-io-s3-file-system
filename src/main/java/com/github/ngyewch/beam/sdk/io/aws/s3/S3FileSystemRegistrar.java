package com.github.ngyewch.beam.sdk.io.aws.s3;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.FileSystemRegistrar;
import org.apache.beam.sdk.options.PipelineOptions;

import javax.annotation.Nullable;

public class S3FileSystemRegistrar implements FileSystemRegistrar {

    @Override
    public Iterable<FileSystem> fromOptions(@Nullable PipelineOptions options) {
        final S3FileSystemOptions fileSystemOptions = options.as(S3FileSystemOptions.class);
        return ImmutableList.of(new S3FileSystem(fileSystemOptions));
    }
}
