package com.solace.demo.flink.event;

import java.util.Objects;

public class RecordWithKey {
    private static final long serialVersionUID = 1L;
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        RecordWithKey that = (RecordWithKey) o;
        return key == that.key && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, message);
    }

    int key;
    String message;

    public void setKey(int key) {
        this.key = key;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getKey() {
        return key;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return String.format("key is %d, message is \n%s", key, message);
    }
}
