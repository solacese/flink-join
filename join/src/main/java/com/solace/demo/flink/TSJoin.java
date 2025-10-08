package com.solace.demo.flink;

import com.solace.demo.flink.event.TimeRecordWithKey;
import com.solace.demo.flink.event.TimeRecordWithKeyDeserializationSchema;
import com.solace.demo.flink.event.TimeRecordWithKeySerializationSchema;
import com.solace.demo.flink.sink.PSPConnectionConfig;
import com.solace.demo.flink.sink.PSPSink;
import com.solacecoe.connectors.flink.streaming.SolaceStreamingSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 *  Using the .join() operator to execute an inner join on the two input streams.
 *  Note that if there is more than one record with key X in streams 1 and 2 in a single time window,
 *  join() will create multiple joined records (i.e. no aggregation).
 */
public class TSJoin {
    private static final Logger LOG = LoggerFactory.getLogger(TSJoin.class);

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

        DataStream<TimeRecordWithKey> stream1 = getEventStream(env, "merge-1-q");
        DataStream<TimeRecordWithKey> stream2 = getEventStream(env, "merge-2-q");

        final StringBuffer mergedMsg = new StringBuffer(1024);  // for assembling our joined text

        DataStream<TimeRecordWithKey> outStream = stream1.join(stream2)
                .where(TimeRecordWithKey::getKey)
                .equalTo(TimeRecordWithKey::getKey)
                .window(TumblingTimeWindows.of(Duration.ofSeconds(20)))
                .apply(new JoinFunction<>() {
                    @Override
                    public TimeRecordWithKey join(TimeRecordWithKey rk1, TimeRecordWithKey rk2) {
                        TimeRecordWithKey rk = new TimeRecordWithKey();
                        rk.setKey(rk1.getKey());
                        mergedMsg.setLength(0);
                        mergedMsg.append("  msg 1:\n");
                        mergedMsg.append(rk1.getMessage());
                        mergedMsg.append("\n  msg 2:\n");
                        mergedMsg.append(rk2.getMessage());
                        mergedMsg.append('\n');
                        rk.setMessage(mergedMsg.toString());
                        LOG.info("I just merged records for key " + rk1.getKey());
                        return rk;
                    }
                });

        // send things to Solace as well
        PSPSink<TimeRecordWithKey> solaceSink = getPSPSink(env);
        outStream.addSink(solaceSink);

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.execute("TSMessageJoinJob");
    }

    /**
     * Creates a typed DataStream for a given queue name; just a helper because for joining we need two of those :)
     * @param env current stream execution environment
     * @param queuename  the Solace queuename to read from; note that we won't get an error yet if the queue doesn't exist, this will only happen on the first read attempt (I think).
     * @return
     */
    private DataStream<TimeRecordWithKey> getEventStream(
            StreamExecutionEnvironment env,
            String queuename) {
        SolaceStreamingSource source = SolaceStreamingSource.newBuilder()
                .withDeserializationSchema(new TimeRecordWithKeyDeserializationSchema())
                .withHostName(hostname)
                .withMessageVPN(vpn)
                .withUsername(username)
                .withPassword(password)
                .withQueue(queuename)
                .build();

        LOG.info("create stream");
        SingleOutputStreamOperator myStream = env
                .addSource(source)
                .name(queuename)
                .returns(TimeRecordWithKey.class);

        return myStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<TimeRecordWithKey>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<>() {
                    @Override
                    public long extractTimestamp(TimeRecordWithKey recordWithKey, long l) {
                        return recordWithKey.getTs();
                    }
                }));
    }

    /**
     * creates a Solace (= PubSubPlus, hence the 'PSP') sink; it will publish records of type T to a topic which deducted via a TopicBuilder interface.
      * @param env execution environment
     * @return
     */
    private PSPSink<TimeRecordWithKey> getPSPSink(StreamExecutionEnvironment env) {
        LOG.info("create PSP sink configuration");
        PSPConnectionConfig config = new PSPConnectionConfig.Builder()
                .setHost(hostname)
                .setVpn(vpn)
                .setUserName(username)
                .setPassword(password)
                .build();

        PSPSink<TimeRecordWithKey> psink = new PSPSink<> (
                config,
            new TimeRecordWithKeySerializationSchema(),
                (TimeRecordWithKey rk) -> String.format("solace/samples/joined/%d", rk.getKey()));
        return psink;
    }

    public static void main(String[] args) {
        TSJoin theJob = new TSJoin();
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

        theJob.checkPointLocation = "file:///tmp/checkpointJ.flink";

        try {
            theJob.run();
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}
