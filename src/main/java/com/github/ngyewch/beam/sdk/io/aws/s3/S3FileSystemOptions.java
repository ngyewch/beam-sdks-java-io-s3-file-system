package com.github.ngyewch.beam.sdk.io.aws.s3;

import org.apache.beam.sdk.options.PipelineOptions;

public interface S3FileSystemOptions extends PipelineOptions {

    String getAwsProfileId();

    void setAwsProfileId(String awsProfileId);

    String getAwsAccessKeyId();

    void setAwsAccessKeyId(String awsAccessKeyId);

    String getAwsSecretAccessKey();

    void setAwsSecretAccessKey(String awsSecretAccessKey);

    String getAwsRegion();

    void setAwsRegion(String awsRegion);

    Integer getAwsS3MaxCacheAgeInSeconds();

    void setAwsS3MaxCacheAgeInSeconds(Integer awsS3MaxCacheAgeInSeconds);
}
