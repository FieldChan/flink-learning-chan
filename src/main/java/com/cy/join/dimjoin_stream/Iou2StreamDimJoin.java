package com.cy.join.dimjoin_stream;

import com.alibaba.fastjson.JSONObject;
import com.cy.common.entity.InstallmentOrder;
import com.cy.common.entity.Iou;
import com.cy.common.entity.LoanOrder;
import com.cy.common.entity.OrderDetail;
import com.cy.common.sink.IouRepalceSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

public class Iou2StreamDimJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        DataStream<String> installmentStream = env
                .addSource(new FlinkKafkaConsumer<>("installment", new SimpleStringSchema(), properties));

        installmentStream.map(new RichMapFunction<String, Iou>(){
            PreparedStatement ps;
            private Connection connection;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/world", "root", "516516");
            }

            @Override
            public Iou map(String value) throws Exception {
                InstallmentOrder installmentOrder = JSONObject.parseObject(value, InstallmentOrder.class);
                //根据order_id查询iou
                String sql = "select order_id, down_payment, order_amt, update_time from iou where order_id = ?";
                ps = this.connection.prepareStatement(sql);
                ps.setLong(1, installmentOrder.getOrder_id());
                ResultSet resultSet = ps.executeQuery();
                Iou iou = null;
                if(resultSet.isBeforeFirst()){
                    while (resultSet.next()){
                        iou = new Iou((Long)resultSet.getObject(1), resultSet.getObject(2)==null?null:resultSet.getDouble(2), resultSet.getObject(3)==null?null:resultSet.getDouble(3), (Long) resultSet.getObject(4));
                    }
                }else {
                    iou = new Iou();
                }
                //替换installment的字段
                iou.setOrder_id(installmentOrder.getOrder_id());
                iou.setDown_Payment(installmentOrder.getDown_Payment());
                iou.setUpdate_time(installmentOrder.getUpdate_time());
                return iou;
            }

            @Override
            public void close() throws Exception {
                super.close();
                //关闭连接和释放资源
                if (connection != null) {
                    connection.close();
                }
                if (ps != null) {
                    ps.close();
                }
            }
        }).addSink(new IouRepalceSink());

        DataStream<String> orderStream = env
                .addSource(new FlinkKafkaConsumer<>("order", new SimpleStringSchema(), properties));

        orderStream.map(new RichMapFunction<String, Iou>(){
            PreparedStatement ps;
            private Connection connection;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/world", "root", "516516");
            }

            @Override
            public Iou map(String value) throws Exception {
                LoanOrder loanOrder = JSONObject.parseObject(value, LoanOrder.class);
                //根据order_id查询iou
                String sql = "select order_id, down_payment, order_amt, update_time from iou where order_id = ?";
                ps = this.connection.prepareStatement(sql);
                ps.setLong(1, loanOrder.getOrder_id());
                ResultSet resultSet = ps.executeQuery();
                Iou iou = null;
                if(resultSet.isBeforeFirst()){
                    while (resultSet.next()){
                        iou = new Iou((Long)resultSet.getObject(1), resultSet.getObject(2)==null?null:resultSet.getDouble(2), resultSet.getObject(3)==null?null:resultSet.getDouble(3), (Long) resultSet.getObject(4));
                    }
                }else {
                    iou = new Iou();
                }
                //替换installment的字段
                iou.setOrder_id(loanOrder.getOrder_id());
                iou.setOrder_amt(loanOrder.getOrder_amt());
                iou.setUpdate_time(loanOrder.getUpdate_time());
                return iou;
            }

            @Override
            public void close() throws Exception {
                super.close();
                //关闭连接和释放资源
                if (connection != null) {
                    connection.close();
                }
                if (ps != null) {
                    ps.close();
                }
            }
        }).addSink(new IouRepalceSink());



        env.execute("iou");

    }



}
