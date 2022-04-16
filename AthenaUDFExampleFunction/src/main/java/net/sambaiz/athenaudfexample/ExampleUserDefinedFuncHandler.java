package net.sambaiz.athenaudfexample;

import com.amazonaws.athena.connector.lambda.handlers.UserDefinedFunctionHandler;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

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
