package com.solace.demo.flink;

import com.solace.demo.flink.event.RecordWithKey;
import com.solace.demo.flink.event.RecordWithKeyDeserializationSchema;
import com.solace.demo.flink.event.RecordWithKeySerializationSchema;
import com.solacecoe.connectors.flink.streaming.SolaceStreamingSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;

// this is temporary workaround until the CoE has a sink
import com.solace.demo.flink.sink.PSPSink;
import com.solace.demo.flink.sink.PSPConnectionConfig;

import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;


public class MessageJoinJob {
    private static final Logger LOG = LoggerFactory.getLogger(MessageJoinJob.class);

    String hostname;
    String vpn;
    String username;
    String password;

    String checkPointLocation;

    public void run() throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000L);
        env.setStateBackend(new FsStateBackend(checkPointLocation));
        env.enableCheckpointing(10000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStream<RecordWithKey> stream1 = getEventStream(env, "merge-1-q");
        DataStream<RecordWithKey> stream2 = getEventStream(env, "merge-2-q");

        DataStream<RecordWithKey> outStream = stream1.join(stream2)
                .where(record -> record.getKey())
                .equalTo(record -> record.getKey())
                .window(TumblingTimeWindows.of(Duration.ofSeconds(20)))
                .apply(new JoinFunction<RecordWithKey, RecordWithKey, RecordWithKey>() {
                    @Override
                    public RecordWithKey join(RecordWithKey rk1, RecordWithKey rk2) {
                        RecordWithKey rk = new RecordWithKey();
                        rk.setKey(rk1.getKey());
                        rk.setMessage(rk1.getMessage() + '\n' + rk2.getMessage());
                        LOG.info("merged records for key " + rk1.getKey());
                        return rk;
                    }
                });

        // send things to Solace as well
        PSPSink<RecordWithKey> solaceSink = getPSPSink(env);
        outStream.addSink(solaceSink);

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.execute("MessageJoinJob");
    }

    /**
     * Creates a typed DataStream for a given queue name; just a helper because for joining we need two of those :)
     * @param env current stream execution environment
     * @param queuename  the Solace queuename to read from; note that we won't get an error yet if the queue doesn't exist, this will only happen on the first read attempt (I think).
     * @return
     */
    private DataStream<RecordWithKey> getEventStream(
            StreamExecutionEnvironment env,
            String queuename) {
        SolaceStreamingSource source = SolaceStreamingSource.newBuilder()
                .withDeserializationSchema(new RecordWithKeyDeserializationSchema())
                .withHostName(hostname)
                .withMessageVPN(vpn)
                .withUsername(username)
                .withPassword(password)
                .withQueue(queuename)
                .build();

        LOG.info("create stream");
        DataStream<RecordWithKey> myStream = env
                .addSource(source)
                .name(queuename)
                .returns(RecordWithKey.class);

        return myStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<RecordWithKey>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis()));

    }

    /**
     * creates a Solace (= PubSubPlus, hence the 'PSP') sink; it will publish records of type T to a topic which deducted via a TopicBuilder interface.
      * @param env execution environment
     * @return
     */
    private PSPSink<RecordWithKey> getPSPSink(StreamExecutionEnvironment env) {
        LOG.info("create PSP sink configuration");
        PSPConnectionConfig config = new com.solace.demo.flink.sink.PSPConnectionConfig.Builder()
                .setHost(hostname)
                .setVpn(vpn)
                .setUserName(username)
                .setPassword(password)
                .build();

        PSPSink<RecordWithKey> psink = new PSPSink<> (
                config,
            new RecordWithKeySerializationSchema(),
                (RecordWithKey rk) -> String.format("solace/samples/joined/%d", rk.getKey()));
        return psink;
    }

    public static void main(String[] args) throws Exception {
        MessageJoinJob theJob = new MessageJoinJob();
        theJob.hostname = "quantumofsolace:55554";
        theJob.vpn = "default";
        theJob.username = "default";
        theJob.password = "default";

        if(args.length > 0)
            theJob.hostname = args[0];
        if(args.length > 1)
            theJob.vpn = args[1];
        if(args.length > 2)
            theJob.username = args[2];
        if(args.length > 3)
            theJob.username = args[3];

        theJob.checkPointLocation = "file:///tmp/checkpoint.flink";

        try {
            theJob.run();
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}
