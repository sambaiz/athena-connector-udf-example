package net.sambaiz.athena_connector_udf_example;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.*;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ExampleMetadataHandler extends MetadataHandler {
    private static final Logger logger = LoggerFactory.getLogger(ExampleMetadataHandler.class);

    private static final String SOURCE_TYPE = "example";

    public ExampleMetadataHandler()
    {
        super(SOURCE_TYPE);
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request) {
        logger.info("MetadataHandler.doListSchemaNames() with requestType: " + request.getRequestType());

        Set<String> schemas = new HashSet<>();
        schemas.add("sample_db");
        return new ListSchemasResponse(request.getCatalogName(), schemas);
    }

    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request) {
        logger.info("MetadataHandler.doListTables() with requestType: " + request.getRequestType());

        List<TableName> tables = new ArrayList<>();
        tables.add(new TableName(request.getSchemaName(), "sample_table"));
        String nextToken = null;
        return new ListTablesResponse(request.getCatalogName(), tables, nextToken);
    }

    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request) {
        logger.info("MetadataHandler.doGetTable() with tableName: " + request.getTableName());

        Set<String> partitionColNames = new HashSet<>();
        partitionColNames.add("year");

        SchemaBuilder tableSchemaBuilder = SchemaBuilder.newBuilder();
        tableSchemaBuilder
            .addIntField("year")
            .addStructField("foo")
            .addChildField("foo", "bar", Types.MinorType.INT.getType());

        return new GetTableResponse(request.getCatalogName(),
                request.getTableName(),
                tableSchemaBuilder.build(),
                partitionColNames);
    }
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) throws Exception {
        logger.info("MetadataHandler.getPartitions() with schema: " + request.getSchema().toJson());
        for (int year = 2000; year <= 2022; year++) {
            final int yearVal = year;
            blockWriter.writeRows((Block block, int row) -> {
                boolean matched = true;
                matched &= block.setValue("year", row, yearVal);
                return matched ? 1 : 0;
            });
        }
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        String partitionFieldNames = request.getPartitions().getFields().stream()
                .map((a) -> a.getName())
                .collect(Collectors.joining(","));
        logger.info("MetadataHandler.doGetSplits() with requestType: partitionFieldNames: " + partitionFieldNames);

        Set<Split> splits = new HashSet<>();
        Block partitions = request.getPartitions();
        FieldReader year = partitions.getFieldReader("year");
        for (int i = 0; i < partitions.getRowCount(); i++) {
            year.setPosition(i);

            // Splits are parallelizable units of work.
            // For each partition in the request, create 1 or more splits.
            Split split = Split.newBuilder(makeSpillLocation(request), makeEncryptionKey())
                    .add("year", String.valueOf(year.readInteger()))
                    .build();
            splits.add(split);
        }

        return new GetSplitsResponse(request.getCatalogName(), splits);
    }
}
