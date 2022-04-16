package net.sambaiz.athenaudfexample;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.*;
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
        return new ListSchemasResponse(request.getCatalogName(), schemas);
    }

    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request) {
        logger.info("MetadataHandler.doListTables() with requestType: " + request.getRequestType());

        List<TableName> tables = new ArrayList<>();
        String nextToken = null;
        return new ListTablesResponse(request.getCatalogName(), tables, nextToken);
    }

    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request) {
        logger.info("MetadataHandler.doGetTable() with tableName: " + request.getTableName());

        Set<String> partitionColNames = new HashSet<>();
        SchemaBuilder tableSchemaBuilder = SchemaBuilder.newBuilder();
        return new GetTableResponse(request.getCatalogName(),
                request.getTableName(),
                tableSchemaBuilder.build(),
                partitionColNames);
    }
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) throws Exception {
        logger.info("MetadataHandler.getPartitions() with partitionCols: " + request.getSchema().toJson());
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request)
    {
        String partitionFieldNames = request.getPartitions().getFields().stream()
                .map((a) -> a.getName())
                .collect(Collectors.joining(","));
        logger.info("MetadataHandler.doGetSplits() with requestType: partitionFieldNames: " + partitionFieldNames);

        String catalogName = request.getCatalogName();
        Set<Split> splits = new HashSet<>();
        return new GetSplitsResponse(catalogName, splits);
    }
}
