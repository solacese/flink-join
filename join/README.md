# A join via Flink example
Horribly simple demo on joining two event streams by a common key to create a new result stream.
There are three parts to this: a broker, the flink server running our join job and a test data publisher

# Artifacts are here:
## Broker
### Queues
assuming you have a local broker with these values (cloud broker should work as well):
+ cip = localhost:55554
+ vpn = default
+ username = default
+ password = default

### you'll need two queues (currently hard-coded) with these names and subscriptions:
+ queue:  merge-1-q    with subscription `solace/samples/0`
+ queue:  merge-2-q    with subscription `solace/samples/1`
+ [... maybe more queues if you are working with more than two input streams; the data generator will support this.]

## Flink
+ download the not-so-latest [Flink release](https://www.apache.org/dyn/closer.lua/flink/flink-1.20.2/flink-1.20.2-bin-scala_2.12.tgz)
+ unpack (you'll need a local JDK for it, JDK 11 should be good enough).
+ edit `<flink_home>/config/config.yaml` and increase the numberOfTaskSlots: `numberOfTaskSlots: 6`
+ start the local server (it really will start just a single server process): `./bin/start-cluster.sh`
+ Flink UI is now on port 8081 ... there should be no jobs
+ run the 'Join' Flink job with `./bin/flink run -c com.solace.demo.flink.MessageJoinJob <path>/join-1.0-SNAPSHOT.jar localhost:55554 default default default`
+ use the `com.solace.demo.flink.TSJoin` job class for messages with timestamps
+ use the `TSUnionAgg` class for the union+aggregator version; example: `./bin/flink run -c com.solace.demo.flink.TSUnionAgg ${HOME}/src/flink/flink-join/join/target/join-1.0-SNAPSHOT.jar localhost:55554 default default default`
Now there should be one flink job, with 3 tasks

### Join
Merges records from both input streams with matching keys via `join()`function. If there is more than one matching record in the other stream & time window, this will create _two_ result records ... records with the same key are not merged by default.

### Union + Aggregator
Different approach: first merge all input streams into a single stream via `union()` call and then perform a keyed aggregation. This works with more than two input streams.

## test data generator
+ file is send-join-1.0-SNAPSHOT.jar
+ it will generate records of this format:    { "key" : 75, "message number xyz" } ... with a timestamp if needed.
+ and send them to the topics `solace/samples/0` and `solace/samples/1` ... so that messages can be joined by that key ("joined" means appended)
+ Per default there is no timestamp in our test data, so Flink will operate on processing time. Timestamps can be enabled in the test data generator, see below. The TS{XYZ} jobs will only work with timestamps.
+ Command: `java -jar ./join-data-generator-1.0-SNAPSHOT.jar localhost:55554 default default default`
+ use this for messages with timestamps: `java -jar ./join-data-generator-1.0-SNAPSHOT.jar localhost:55554 default default default true`
## (Sidenote) Need files? 
Create a typed file sink:
```
    private StreamingFileSink<RecordWithKey> getFileSink(String outputPath) {
        OutputFileConfig outputConfig = OutputFileConfig
                .builder()
                .withPartPrefix("my-data")
                .withPartSuffix(".txt")
                .build();

        StreamingFileSink<RecordWithKey> s = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<RecordWithKey>("UTF-8"))
                .withOutputFileConfig(outputConfig)
                .build();
        return s;
    }
```

I'll update links once I have a fixed location for these projects.
