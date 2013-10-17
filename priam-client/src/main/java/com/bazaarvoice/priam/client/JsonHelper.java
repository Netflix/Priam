package com.bazaarvoice.priam.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JsonHelper {
    private static final Logger LOG = LoggerFactory.getLogger(JsonHelper.class);

    private static final ObjectMapper JSON = new ObjectMapper();
    public static <T> T fromJson(String string, Class<T> type) {
        try {
            return JSON.readValue(string, type);
        } catch (IOException e) {
            LOG.error("Error deserializing json:\"{}\", to type:\"{}\"", string,type);
            throw new AssertionError(e);
        }
    }

    public static String toJson(Object obj){

        try {
            return JSON.writeValueAsString(obj);
        } catch (IOException e) {
            throw new AssertionError(e);
        }

    }

    public static <T> T deserialized(String json, TypeReference<T> type) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(json, type);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
