package com.solace.demo.flink.function;

import com.solace.demo.flink.event.TimeRecordWithKey;
import org.apache.flink.api.common.functions.AggregateFunction;

public class TimeRecordAggregate implements AggregateFunction<
        TimeRecordWithKey, TimeRecordWithKey, TimeRecordWithKey> {
    private StringBuffer mergedMsg = new StringBuffer(1024);

    @Override
    public TimeRecordWithKey createAccumulator() {
        return new TimeRecordWithKey();
    }

    @Override
    public TimeRecordWithKey add(TimeRecordWithKey trk1, TimeRecordWithKey trk2) {
        TimeRecordWithKey res = new TimeRecordWithKey();
        res.setKey(trk1.getKey());
        mergedMsg.setLength(0);
        mergedMsg.append(trk1.getMessage());
        mergedMsg.append("\n");
        mergedMsg.append(trk2.getMessage());
        res.setMessage(mergedMsg.toString());
        return res;
    }

    @Override
    public TimeRecordWithKey getResult(TimeRecordWithKey trk) {
        return trk;
    }

    @Override
    public TimeRecordWithKey merge(TimeRecordWithKey trk, TimeRecordWithKey acc) {
        return add(trk, acc);
    }
}
