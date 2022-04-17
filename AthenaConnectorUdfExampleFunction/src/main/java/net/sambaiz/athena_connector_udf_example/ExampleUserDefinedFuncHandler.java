package net.sambaiz.athena_connector_udf_example;

import com.amazonaws.athena.connector.lambda.handlers.UserDefinedFunctionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleUserDefinedFuncHandler extends UserDefinedFunctionHandler {
    private static final Logger logger = LoggerFactory.getLogger(ExampleUserDefinedFuncHandler.class);

    private static final String SOURCE_TYPE = "custom";

    public ExampleUserDefinedFuncHandler()
    {
        super(SOURCE_TYPE);
    }

    public Integer plus_one(Integer n) {
        logger.info("UserDefinedFunctionHandler.plus_one() with " + n);
        return n + 1;
    }
}
