package net.sambaiz.athena_connector_udf_example;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintProjector;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.google.common.base.Joiner;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
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

        GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());
        builder.withExtractor("year", (IntExtractor) (Object context, NullableIntHolder value) -> {
            value.isSet = 1;
            value.value = Integer.parseInt(((String[]) context)[0]);
        });
        builder.withFieldWriterFactory("foo",
          (FieldVector vector, Extractor extractor, ConstraintProjector constraint) ->
            (Object context, int rowNum) -> {
                Map<String, Object> eventMap = new HashMap<>();
                eventMap.put("bar", Integer.parseInt(((String[])context)[1]));
                BlockUtils.setComplexValue(vector, rowNum, FieldResolver.DEFAULT, eventMap);
                return true;
            });
        GeneratedRowWriter rowWriter = builder.build();

        int splitYear = recordsRequest.getSplit().getPropertyAsInt("year");
        for (int i = 0; i < 5; i++) {
            String[] data = {
                    String.valueOf(splitYear), /* year */
                    String.valueOf(i) /* foo.bar */
            };
            spiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, data) ? 1 : 0);
        }
    }
}
