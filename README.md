# athena-connector-udf-example

## Deploy

```bash
$ sam build
$ sam deploy --guided
# Cleanup
# aws cloudformation delete-stack --stack-name athena-connector-udf-example 
```

## Run

### MetadataHandler

- query

```sql
describe `lambda:athena-connector-udf-example`.sample_db.sample_table
```

- result

```
foo struct<bar:int>
# Partition Information
# col_name  data_type   comment
year    int
```

- log

```
START RequestId: f527726a-549d-4c23-884e-c3c9ca168ce9 Version: $LATEST
INFO MetadataHandler:239 - doHandleRequest: request[GetTableRequest{queryId=36123e07-c4c9-491b-8d66-78871bcff04a, tableName=TableName{schemaName=sample_db, tableName=sample_table}}]
INFO ExampleMetadataHandler:54 - MetadataHandler.doGetTable() with tableName: TableName{schemaName=sample_db, tableName=sample_table}
INFO MetadataHandler:258 - doHandleRequest: response[GetTableResponse{tableName=TableName{schemaName=sample_db, tableName=sample_table}, schema=Schema<year: Int(32, true), foo: Struct<bar: Int(32, true)>>, partitionColumns=[year], requestType=GET_TABLE, catalogName=lambda:athena-connector-udf-example}]
END RequestId: f527726a-549d-4c23-884e-c3c9ca168ce9
REPORT RequestId: f527726a-549d-4c23-884e-c3c9ca168ce9 Duration: 420.30 ms Billed Duration: 421 ms Memory Size: 512 MB Max Memory Used: 164 MB
```

### MetadataHandler + RecordHandler

- query

```
select * from "lambda:athena-connector-udf-example".sample_db.sample_table where year = 2022
```

- result

```
#	year	foo
1	2022	{bar=0}
2	2022	{bar=1}
3	2022	{bar=2}
4	2022	{bar=3}
5	2022	{bar=4}
```

- log

```
START RequestId: f9bb8788-dd14-4f80-8ff7-d578c7fb6734 Version: $LATEST
INFO MetadataHandler:239 - doHandleRequest: request[GetTableRequest{queryId=c2d58b0d-c8f2-43be-a5e5-0333807de890, tableName=TableName{schemaName=sample_db, tableName=sample_table}}]
INFO ExampleMetadataHandler:54 - MetadataHandler.doGetTable() with tableName: TableName{schemaName=sample_db, tableName=sample_table}
INFO MetadataHandler:258 - doHandleRequest: response[GetTableResponse{tableName=TableName{schemaName=sample_db, tableName=sample_table}, schema=Schema<year: Int(32, true), foo: Struct<bar: Int(32, true)>>, partitionColumns=[year], requestType=GET_TABLE, catalogName=lambda:athena-connector-udf-example}]
END RequestId: f9bb8788-dd14-4f80-8ff7-d578c7fb6734
REPORT RequestId: f9bb8788-dd14-4f80-8ff7-d578c7fb6734 Duration: 388.13 ms Billed Duration: 389 ms Memory Size: 512 MB Max Memory Used: 165 MB

START RequestId: a5e9627b-884f-4f94-89ab-c4c1c3d377af Version: $LATEST
INFO MetadataHandler:239 - doHandleRequest: request[GetTableLayoutRequest{queryId=c2d58b0d-c8f2-43be-a5e5-0333807de890, tableName=TableName{schemaName=sample_db, tableName=sample_table}, constraints=Constraints{summary={year=SortedRangeSet{type=Int(32, true), nullAllowed=false, lowIndexedRanges={Marker{valueBlock=Int(32, true), nullValue=false, valueBlock=2022, bound=EXACTLY}=Range{low=Marker{valueBlock=Int(32, true), nullValue=false, valueBlock=2022, bound=EXACTLY}, high=Marker{valueBlock=Int(32, true), nullValue=false, valueBlock=2022, bound=EXACTLY}}}}}}, schema=Schema<year: Int(32, true), foo: Struct<bar: Int(32, true)>>, partitionCols=[year]}]
INFO ExampleMetadataHandler:72 - MetadataHandler.getPartitions() with schema: { "fields" : [ { "name" : "year", "nullable" : true, "type" : { "name" : "int", "bitWidth" : 32, "isSigned" : true }, "children" : [ ] }, { "name" : "foo", "nullable" : true, "type" : { "name" : "struct" }, "children" : [ { "name" : "bar", "nullable" : true, "type" : { "name" : "int", "bitWidth" : 32, "isSigned" : true }, "children" : [ ] } ] } ] }
INFO MetadataHandler:266 - doHandleRequest: response[GetTableLayoutResponse{tableName=TableName{schemaName=sample_db, tableName=sample_table}, requestType=GET_TABLE_LAYOUT, catalogName=lambda:athena-connector-udf-example}]
END RequestId: a5e9627b-884f-4f94-89ab-c4c1c3d377af
REPORT RequestId: a5e9627b-884f-4f94-89ab-c4c1c3d377af Duration: 1065.14 ms Billed Duration: 1066 ms Memory Size: 512 MB Max Memory Used: 184 MB

START RequestId: 223318ee-a1f9-4b48-885f-cc849c74b360 Version: $LATEST
INFO MetadataHandler:239 - doHandleRequest: request[GetTableLayoutRequest{queryId=c2d58b0d-c8f2-43be-a5e5-0333807de890, tableName=TableName{schemaName=sample_db, tableName=sample_table}, constraints=Constraints{summary={year=SortedRangeSet{type=Int(32, true), nullAllowed=false, lowIndexedRanges={Marker{valueBlock=Int(32, true), nullValue=false, valueBlock=2022, bound=EXACTLY}=Range{low=Marker{valueBlock=Int(32, true), nullValue=false, valueBlock=2022, bound=EXACTLY}, high=Marker{valueBlock=Int(32, true), nullValue=false, valueBlock=2022, bound=EXACTLY}}}}}}, schema=Schema<year: Int(32, true), foo: Struct<bar: Int(32, true)>>, partitionCols=[year]}]
INFO ExampleMetadataHandler:72 - MetadataHandler.getPartitions() with schema: { "fields" : [ { "name" : "year", "nullable" : true, "type" : { "name" : "int", "bitWidth" : 32, "isSigned" : true }, "children" : [ ] }, { "name" : "foo", "nullable" : true, "type" : { "name" : "struct" }, "children" : [ { "name" : "bar", "nullable" : true, "type" : { "name" : "int", "bitWidth" : 32, "isSigned" : true }, "children" : [ ] } ] } ] }
INFO MetadataHandler:266 - doHandleRequest: response[GetTableLayoutResponse{tableName=TableName{schemaName=sample_db, tableName=sample_table}, requestType=GET_TABLE_LAYOUT, catalogName=lambda:athena-connector-udf-example}]
END RequestId: 223318ee-a1f9-4b48-885f-cc849c74b360
REPORT RequestId: 223318ee-a1f9-4b48-885f-cc849c74b360 Duration: 8.27 ms Billed Duration: 9 ms Memory Size: 512 MB Max Memory Used: 184 MB

START RequestId: 2734871e-33f0-4922-9381-f3996b4d8ccd Version: $LATEST
INFO MetadataHandler:239 - doHandleRequest: request[GetSplitsRequest{queryId=c2d58b0d-c8f2-43be-a5e5-0333807de890, tableName=TableName{schemaName=sample_db, tableName=sample_table}, partitionCols=[year], requestType=GET_SPLITS, catalogName=lambda:athena-connector-udf-example, partitions=Block{rows=1, year=[2022]}, constraints=Constraints{summary={year=SortedRangeSet{type=Int(32, true), nullAllowed=false, lowIndexedRanges={Marker{valueBlock=Int(32, true), nullValue=false, valueBlock=2022, bound=EXACTLY}=Range{low=Marker{valueBlock=Int(32, true), nullValue=false, valueBlock=2022, bound=EXACTLY}, high=Marker{valueBlock=Int(32, true), nullValue=false, valueBlock=2022, bound=EXACTLY}}}}}}, continuationToken=null}]
INFO ExampleMetadataHandler:89 - MetadataHandler.doGetSplits() with requestType: partitionFieldNames: year
INFO MetadataHandler:274 - doHandleRequest: response[GetSplitsResponse{splitSize=1, continuationToken='null'}]
END RequestId: 2734871e-33f0-4922-9381-f3996b4d8ccd
REPORT RequestId: 2734871e-33f0-4922-9381-f3996b4d8ccd Duration: 200.77 ms Billed Duration: 201 ms Memory Size: 512 MB Max Memory Used: 184 MB

START RequestId: 400bf62f-bf7c-4355-81b9-ab9f56c0d624 Version: $LATEST
INFO RecordHandler:154 - doHandleRequest: request[ReadRecordsRequest{queryId=c2d58b0d-c8f2-43be-a5e5-0333807de890, tableName=TableName{schemaName=sample_db, tableName=sample_table}, schema=Schema<year: Int(32, true), foo: Struct<bar: Int(32, true)>>, split=Split{spillLocation=S3SpillLocation{bucket='null', key='athena-federation-spill/c2d58b0d-c8f2-43be-a5e5-0333807de890/c11f0906-9c36-4116-8193-f85c589b33c5', directory=true}, encryptionKey=<hidden>, properties={year=2022}}, requestType=READ_RECORDS, catalogName=lambda:athena-connector-udf-example, maxBlockSize=16000000, maxInlineBlockSize=3465000, constraints=Constraints{summary={year=SortedRangeSet{type=Int(32, true), nullAllowed=false, lowIndexedRanges={Marker{valueBlock=Int(32, true), nullValue=false, valueBlock=2022, bound=EXACTLY}=Range{low=Marker{valueBlock=Int(32, true), nullValue=false, valueBlock=2022, bound=EXACTLY}, high=Marker{valueBlock=Int(32, true), nullValue=false, valueBlock=2022, bound=EXACTLY}}}}}}}]
INFO RecordHandler:184 - doReadRecords: Schema<year: Int(32, true), foo: Struct<bar: Int(32, true)>>:S3SpillLocation{bucket='null', key='athena-federation-spill/c2d58b0d-c8f2-43be-a5e5-0333807de890/c11f0906-9c36-4116-8193-f85c589b33c5', directory=true}
INFO ExampleRecordHandler:41 - RecordHandler.readWithConstraint() with splitProperties: year=2022
INFO GeneratedRowWriter:129 - recompile: Detected a new block, rebuilding field writers so they point to the correct Arrow vectors.
INFO S3BlockSpiller:237 - getBlock: Inline Block size[43] bytes vs 3465000
INFO S3BlockSpiller:284 - close: Spilled a total of 0 bytes in 4798 ms
INFO RecordHandler:159 - doHandleRequest: response[ReadRecordsResponse{records=Block{rows=5, year=[2022, 2022, 2022, 2022, 2022], foo=[{[bar : 0]}, {[bar : 1]}, {[bar : 2]}, {[bar : 3]}, {[bar : 4]}]}, requestType=READ_RECORDS, catalogName=lambda:athena-connector-udf-example}]
END RequestId: 400bf62f-bf7c-4355-81b9-ab9f56c0d624
REPORT RequestId: 400bf62f-bf7c-4355-81b9-ab9f56c0d624 Duration: 4956.29 ms Billed Duration: 4957 ms Memory Size: 512 MB Max Memory Used: 207 MB
```

### UserDefinedFunction

- query

```sql
USING 
    EXTERNAL FUNCTION plus_one(value INT) RETURNS INT LAMBDA 'athena-connector-udf-example'
SELECT plus_one(100)
```

- result 

```
101
```

- log

```
START RequestId: 9f6de11b-b7c3-41aa-8079-752c65bc8553 Version: $LATEST
INFO UserDefinedFunctionHandler:145 - doHandleRequest: request[com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionRequest@fd6b5b22]
INFO UserDefinedFunctionHandler:248 - Found UDF method plus_one with input types [[class java.lang.Integer]] and output types [java.lang.Integer]
INFO GeneratedRowWriter:129 - recompile: Detected a new block, rebuilding field writers so they point to the correct Arrow vectors.
INFO ExampleUserDefinedFuncHandler:30 - UserDefinedFunctionHandler.plus_one() with 100
INFO UserDefinedFunctionHandler:147 - doHandleRequest: response[com.amazonaws.athena.connector.lambda.udf.UserDefinedFunctionResponse@91cd1e3d]
END RequestId: 9f6de11b-b7c3-41aa-8079-752c65bc8553
REPORT RequestId: 9f6de11b-b7c3-41aa-8079-752c65bc8553 Duration: 58.51 ms Billed Duration: 59 ms Memory Size: 512 MB Max Memory Used: 210 MB
```

## Article

([ja](https://www.sambaiz.net/article/402/)/[en](https://www.sambaiz.net/en/article/402/)) Implement Athena's data source connectors and user defined functions (UDF) - sambaiz-net
