/*
 * which license is this under again?
 */

package com.solace.demo.flink.sink;

import com.solace.messaging.MessagingService;
import com.solace.messaging.PubSubPlusClientException;
import com.solace.messaging.publisher.DirectMessagePublisher;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.publisher.OutboundMessageBuilder;
import com.solace.messaging.resources.Topic;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Sink for publishing data into PubSub+. not ready for parallel operation yet (see setRuntimeContext)
 *
 * @param <IN>
 */
public class PSPSink<IN> extends RichSinkFunction<IN> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(PSPSink.class);

    private final PSPConnectionConfig PSPConnectionConfig;
    protected transient MessagingService messagingService;
    protected transient DirectMessagePublisher publisher;
    protected transient OutboundMessageBuilder messageBuilder;
    protected SerializationSchema<IN> schema;

    protected TopicBuilder<IN> topicBuilder;
    private boolean logFailuresOnly = false;

    /**
     */
    public PSPSink(
            PSPConnectionConfig connectionConfig,
            SerializationSchema<IN> schema,
            TopicBuilder<IN> topicBuilder) {
        this.PSPConnectionConfig = connectionConfig;
        this.schema = schema;
        LOG.info("schema handler is {}", this.schema.getClass().getName());
        this.topicBuilder = topicBuilder;
        LOG.info("topic builder is {}", this.topicBuilder.getClass().getName());
    }

    /**
     * Defines whether the producer should fail on errors, or only log them. If this is set to true,
     * then exceptions will be only logged, if set to false, exceptions will be eventually thrown
     * and cause the streaming program to fail (and enter recovery).
     *
     * @param logFailuresOnly The flag to indicate logging-only on exceptions.
     */
    public void setLogFailuresOnly(boolean logFailuresOnly) {
        this.logFailuresOnly = logFailuresOnly;
    }

    @Override
    public void open(Configuration config) throws Exception {
        schema.open(
                RuntimeContextInitializationContextAdapters.serializationAdapter(
                        getRuntimeContext(), metricGroup -> metricGroup.addGroup("user")));

        try {
            messagingService = PSPConnectionConfig.getMessagingService();
            LOG.info("MessagingService connected, next is publisher.");
            publisher = messagingService.createDirectMessagePublisherBuilder()
                    .onBackPressureWait(1000)
                    .build()
                    .start();

            LOG.info("Publisher here, next is message builder");
            messageBuilder = messagingService.messageBuilder();
            LOG.info("Message builder is here");
        } catch (PubSubPlusClientException e) {
            throw new RuntimeException("Error while creating the channel", e);
        }
    }

    /**
     * Called when new data arrives to the sink, and forwards it to RMQ.
     *
     * @param value The incoming data
     */
    @Override
    public void invoke(IN value, Context context) {
        byte[] msg = schema.serialize(value);
        String topicString = topicBuilder.extractTopic(value);

        OutboundMessage message = messageBuilder
            .withSenderId("flink").build(msg);
        publisher.publish(message, Topic.of(topicString));
    }

    @Override
    public void close() {
        Exception t = null;

        try {
            if (publisher != null) {
                publisher.terminate(5000L);
            }

            if (messagingService != null) {
                messagingService.disconnect();
            }
        } catch(Exception e) {
            LOG.error("Error while closing connection", e);
            t = e;
        }
        if (t != null) {
            throw new RuntimeException(
                    "Error while closing PSP connection with "
                            + " at "
                            + PSPConnectionConfig.getHost(),
                    t);
        }
    }
}
