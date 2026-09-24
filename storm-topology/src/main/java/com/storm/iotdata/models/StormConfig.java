package com.storm.iotdata.models;

import java.util.Arrays;
import java.util.List;

public class StormConfig {

    // =====================================================================
    // TIME SLICES (Punctuation Generation)
    // =====================================================================
    // Window sizes, in minutes, used to generate punctuation events
    // across the topology. Each value defines a separate punctuation stream.
    private static final List<Integer> timeSliceMinutes = Arrays.asList(1, 5, 15, 60, 120);

    public static List<Integer> getTimeSliceMinutes() {
        return timeSliceMinutes;
    }

    // =====================================================================
    // SPOUT-DATA
    // =====================================================================
    // MQTT broker URI that the spout connects to.
    private static final String brokerUri = "tcp://mqtt-broker:1883";

    // MQTT topic consumed by the spout.
    private static final String brokerTopic = "iot-data";

    // MQTT subscription QoS used by the spout.
    private static final int qos = 0;

    // Maximum number of stream events emitted by one nextTuple() call.
    private static final int maxEmitPerNextTuple = 100;

    // Maximum number of stream events buffered before new messages are dropped.
    private static final int queueCapacity = 10000;

    // Storm stream id used for data tuples.
    private static final String streamIdData = "data";

    // MQTT property value that identifies a load event.
    private static final int propertyLoad = 1;

    // MQTT connection timeout in seconds.
    private static final int connectionTimeoutSeconds = 10;

    public static String getBrokerUri() {
        return brokerUri;
    }

    public static String getBrokerTopic() {
        return brokerTopic;
    }

    public static int getQos() {
        return qos;
    }

    public static int getMaxEmitPerNextTuple() {
        return maxEmitPerNextTuple;
    }

    public static int getQueueCapacity() {
        return queueCapacity;
    }

    public static String getStreamIdData() {
        return streamIdData;
    }

    public static int getPropertyLoad() {
        return propertyLoad;
    }

    public static int getConnectionTimeoutSeconds() {
        return connectionTimeoutSeconds;
    }
}