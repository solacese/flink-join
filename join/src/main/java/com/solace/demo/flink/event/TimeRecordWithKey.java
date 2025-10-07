package com.solace.demo.flink.event;

import java.util.Objects;

public class TimeRecordWithKey {
    int     key;
    String  message;
    long    ts;

    public TimeRecordWithKey() {
        key = -1;
        message = "";
        ts = System.currentTimeMillis();
    }
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        TimeRecordWithKey that = (TimeRecordWithKey) o;
        return getTs() == that.getTs() && key == that.key && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, message, getTs());
    }

    public String getMessage() {
        return message;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getKey() {
        return key;
    }

    @Override
    public String toString() {
        return String.format("key is %d, message is \n%s", key, message);
    }

    public long getTs() {  return ts;   }

    public void setTs(long ts) {  this.ts = ts;  }
}
