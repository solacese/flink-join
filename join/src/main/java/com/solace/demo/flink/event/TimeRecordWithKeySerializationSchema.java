package com.solace.demo.flink.event;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeRecordWithKeySerializationSchema implements SerializationSchema<TimeRecordWithKey> {
    private static final Logger LOG = LoggerFactory.getLogger(TimeRecordWithKeySerializationSchema.class);
    ObjectMapper objectMapper = null;

    @Override
    public void open(InitializationContext context) throws Exception {
        SerializationSchema.super.open(context);
        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(TimeRecordWithKey recordWithKey) {
        byte[] result = {};
        try {
            result = objectMapper.writeValueAsBytes(recordWithKey);
        } catch (JsonProcessingException e) {
            LOG.error(e.getMessage(), e);
        }
        return result;
    }
}
