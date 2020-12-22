package com.cy.join.dimjoin_stream.producer;

import com.cy.common.utils.ExecutionEnvUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Random;

/**
 */
public class OrderKafkaProducer {

    public static final Random random = new Random();

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.setParallelism(1);
        env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> context) throws Exception {
//                String outline = String.format("{\"order_id\":\"%s\",\"order_amt\":\"%s\",\"update_time\":\"%s\"}", new Random().nextInt(10),new Random().nextDouble(), System.currentTimeMillis());
//                String outline = String.format("{\"order_id\":\"%s\",\"order_amt\":\"%s\",\"update_time\":\"%s\"}", 3,new Random().nextDouble(), System.currentTimeMillis());
//                context.collect(outline);
                while(true){
                    String outline = String.format("{\"order_id\":\"%s\",\"order_amt\":\"%s\",\"update_time\":\"%s\"}", new Random().nextInt(10),new Random().nextDouble(), System.currentTimeMillis());
                    context.collect(outline);//每n秒产生一条数据
                    Thread.sleep(1000);
                }
            }
            @Override
            public void cancel() {
            }
        })
                .addSink(new FlinkKafkaProducer<>(
                        "localhost:9092",
                        "order",//small_order big_order
                        new SimpleStringSchema()
                ));

        env.execute("InstallmentKafkaProducer");
    }
}
