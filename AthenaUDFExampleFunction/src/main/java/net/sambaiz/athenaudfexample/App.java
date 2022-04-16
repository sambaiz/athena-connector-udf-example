package net.sambaiz.athenaudfexample;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

public class App extends CompositeHandler {
    public App() {
        super(new ExampleMetadataHandler(), new ExampleRecordHandler(), new ExampleUserDefinedFuncHandler());
    }
}
