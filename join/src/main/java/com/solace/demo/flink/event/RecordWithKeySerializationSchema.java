package com.solace.demo.flink.event;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class RecordWithKeySerializationSchema implements SerializationSchema<RecordWithKey> {
    private static final Logger LOG = LoggerFactory.getLogger(RecordWithKeySerializationSchema.class);
    ObjectMapper objectMapper = null;

    @Override
    public void open(InitializationContext context) throws Exception {
        SerializationSchema.super.open(context);
        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(RecordWithKey recordWithKey) {
        byte[] result = {};
        try {
            result = objectMapper.writeValueAsBytes(recordWithKey);
        } catch (JsonProcessingException e) {
            LOG.error(e.getMessage(), e);
        }
        return result;
    }
}
