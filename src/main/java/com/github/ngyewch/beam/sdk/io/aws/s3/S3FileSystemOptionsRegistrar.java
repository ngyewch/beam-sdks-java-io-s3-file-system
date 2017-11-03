package com.github.ngyewch.beam.sdk.io.aws.s3;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;

public class S3FileSystemOptionsRegistrar implements PipelineOptionsRegistrar {

    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
        return ImmutableList.of(S3FileSystemOptions.class);
    }
}
