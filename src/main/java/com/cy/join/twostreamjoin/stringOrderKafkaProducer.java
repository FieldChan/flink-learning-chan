package com.cy.join.twostreamjoin;

import com.cy.common.utils.ExecutionEnvUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Random;

/**
 */
public class stringOrderKafkaProducer {

    public static final Random random = new Random();

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.setParallelism(1);
        env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> context) throws Exception {
                    String outline = String.format("{\"cityId\":\"%s\",\"goodsId\":5,\"orderId\":\"orderId:104\",\"price\":8554,\"createTime\":\"%s\",\"userId\":\"UserId:9311\"}", new Random().nextInt(100), System.currentTimeMillis());
                    context.collect(outline);
            }
            @Override
            public void cancel() {
            }
        })
                .addSink(new FlinkKafkaProducer<>(
                        "localhost:9092",
                        "small_order",//small_order big_order
                        new SimpleStringSchema()
                ));

        env.execute("stringKafkaProducer");
    }
}
