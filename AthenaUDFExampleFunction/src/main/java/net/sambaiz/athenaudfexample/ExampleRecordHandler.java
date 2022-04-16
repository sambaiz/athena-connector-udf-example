package net.sambaiz.athenaudfexample;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class ExampleRecordHandler extends RecordHandler {
    private static final Logger logger = LoggerFactory.getLogger(ExampleRecordHandler.class);

    private static final String SOURCE_TYPE = "example";

    public ExampleRecordHandler()
    {
        super(AmazonS3ClientBuilder.defaultClient(), AWSSecretsManagerClientBuilder.defaultClient(), AmazonAthenaClientBuilder.defaultClient(), SOURCE_TYPE);
    }

    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker) throws IOException {
        String splitProperties = Joiner.on(",").withKeyValueSeparator("=").join(recordsRequest.getSplit().getProperties());
        logger.info("RecordHandler.readWithConstraint() with splitProperties: " + splitProperties);
    }
}
