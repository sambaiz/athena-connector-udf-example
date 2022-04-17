package net.sambaiz.athena_connector_udf_example;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

public class App extends CompositeHandler {
    public App() {
        super(new ExampleMetadataHandler(), new ExampleRecordHandler(), new ExampleUserDefinedFuncHandler());
    }
}
