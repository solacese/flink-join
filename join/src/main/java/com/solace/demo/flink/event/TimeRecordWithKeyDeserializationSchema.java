package com.solace.demo.flink.event;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TimeRecordWithKeyDeserializationSchema extends AbstractDeserializationSchema<TimeRecordWithKey> {
    private final static Logger LOG = LoggerFactory.getLogger(TimeRecordWithKeyDeserializationSchema.class);
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public TimeRecordWithKey deserialize(byte[] message) throws IOException {
        try {
            return mapper.readValue(message, TimeRecordWithKey.class);
        } catch (Exception e) {
            LOG.error("Couldn't deserialize RecordWithKey ", e);
            throw new IOException(e.getMessage());
        }
    }
}
