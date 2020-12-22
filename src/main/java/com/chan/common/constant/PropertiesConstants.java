package com.chan.common.constant;

public class PropertiesConstants {

    public static final String PROPERTIES_FILE_NAME = "/application.properties";
    public static final String KAFKA_BROKERS = "kafka.brokers";
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String KAFKA_ZOOKEEPER_CONNECT = "kafka.zookeeper.connect";
    public static final String KAFKA_GROUP_ID = "kafka.group.id";

    public static final String CONSUMER_FROM_TIME = "consumer.from.time";
    public static final String STREAM_PARALLELISM = "stream.parallelism";
    public static final String STREAM_SINK_PARALLELISM = "stream.sink.parallelism";
    public static final String STREAM_DEFAULT_PARALLELISM = "stream.default.parallelism";
    public static final String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";
    public static final String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";

    public static final String STRINGSERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String KEYSERIALIZER = "key.serializer";
    public static final String VALUESERIALIZER = "value.serializer";

    public static final String STUDENT_TOPIC = "student";

    public static final String CY = "chenye";

    public static final String PASSWORD = "password";
    public static final int AGE = 18;
}
