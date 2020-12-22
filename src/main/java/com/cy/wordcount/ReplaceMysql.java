package com.cy.wordcount;

import com.alibaba.fastjson.JSONObject;
import com.cy.common.entity.OrderDetail;
import com.cy.common.sink.Sink2MySQLOrderDetail;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class ReplaceMysql {


    public static void main(String[] args) throws Exception {

        // 创建 execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("big_order", new SimpleStringSchema(), properties));

        stream.map(s ->
            JSONObject.parseObject(s, OrderDetail.class)
        ).addSink(new Sink2MySQLOrderDetail());

        env.execute("rep");
    }



}
