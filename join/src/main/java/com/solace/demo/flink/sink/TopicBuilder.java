package com.solace.demo.flink.sink;

import java.io.Serializable;

public interface TopicBuilder<T> extends Serializable {
    String extractTopic(T record);
}
