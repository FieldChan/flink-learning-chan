package com.chan.join.twostreamjoin;

import com.alibaba.fastjson.JSONObject;
import com.chan.common.data.OrderGenerator;
import com.chan.common.entity.Order;
import com.chan.common.utils.ExecutionEnvUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Objects;
import java.util.Random;

/**
 */
public class smallOrderKafkaProducer {

    public static final Random random = new Random();

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.setParallelism(1);
        DataStream<Order> orderStream = env.addSource(new OrderGenerator())
                .filter(Objects::nonNull);
        orderStream.map(order ->
        {order.setOrderTag("smallorder");
            return order;
        }).map(order -> JSONObject.toJSONString(order))
                .addSink(new FlinkKafkaProducer(
                        "localhost:9092",
                        "small_order",
                        new SimpleStringSchema()
                )).name("flink-smallorder-kafka");

        env.execute("flink small_order");
    }
}
