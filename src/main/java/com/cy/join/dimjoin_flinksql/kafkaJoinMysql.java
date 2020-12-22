package com.cy.join.dimjoin_flinksql;

import com.cy.common.entity.OrderDetail;
import com.cy.common.sink.Sink2MySQLOrderDetail;
import com.cy.common.utils.CheckpointUtil;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class kafkaJoinMysql {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        bsEnv.setParallelism(1);
        bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        CheckpointUtil.setFsStateBackend(bsEnv);

        //设置重启策略：不重启
//        blinkStreamEnv.setRestartStrategy(RestartStrategies.noRestart());

        String ddlMysqlSource = "CREATE TABLE city (\n" +
                "  ID INT,\n" +
                "  Name VARCHAR,\n" +
                "  CountryCode CHAR,\n" +
                "  District CHAR,\n" +
                "  Population INT,\n" +
                "  PRIMARY KEY (ID) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector.type' = 'jdbc',\n" +
                "    'connector.url' = 'jdbc:mysql://localhost:3306/world',\n" +
                "    'connector.table' = 'city',\n" +
                "    'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
                "    'connector.username' = 'root',\n" +
                "    'connector.password' = '516516'\n" +
                ")";

        String ddlKafkaSource = "CREATE TABLE big_order (\n" +
                "    orderId STRING,\n" +
                "    cityId INT, \n" +
                "    goodsId INT, \n" +
                "    userId STRING,\n" +
                "    price INT,\n" +
                "    createTime BIGINT,\n" +
                "    proctime as PROCTIME()\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka-0.11',\n" +
                " 'topic' = 'big_order',\n" +
                " 'properties.bootstrap.servers' = 'localhost:9092',\n" +
                " 'properties.group.id' = 'testGroup',\n" +
                " 'format' = 'json',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'json.fail-on-missing-field' = 'false',\n" +
                " 'json.ignore-parse-errors' = 'true'\n" +
                ")";

        String ddlSink = "CREATE TABLE order_detail (\n" +
                "    orderId STRING,\n" +
                "    cityId INT, \n" +
                "    cityName STRING,\n" +
                "    goodsId INT, \n" +
                "    userId STRING,\n" +
                "    price INT,\n" +
                "    createTime BIGINT,\n" +
                "    PRIMARY KEY (orderId) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://localhost:3306/world',\n" +
                "    'table-name' = 'order_detail',\n" +
                "    'driver' = 'com.mysql.jdbc.Driver',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '516516',\n" +
                "    'sink.buffer-flush.interval' = '1s'\n" +
                ")";

//        String sql = "select * from city  limit 10";
//        String sql1 = "select * from big_order ";
//        String sql2 = "select * from order_detail ";
//        String sql2 = "INSERT INTO order_detail\n" +
//                "SELECT\n" +
//                "    a.orderId,\n" +
//                "    a.cityId,\n" +
//                "    b.Name As cityName,\n" +
//                "    a.goodsId,\n" +
//                "    a.userId,\n" +
//                "    a.price,\n" +
//                "    a.createTime\n" +
//                "FROM big_order As a\n" +
//                "inner join city FOR SYSTEM_TIME AS OF a.proctime AS b\n" +
//                "ON a.cityId = b.ID";
        String sql2 = "SELECT\n" +
                "    a.orderId,\n" +
                "    a.cityId,\n" +
                "    b.Name As cityName,\n" +
                "    a.goodsId,\n" +
                "    a.userId,\n" +
                "    a.price,\n" +
                "    a.createTime\n" +
                "FROM big_order As a\n" +
                "inner join city FOR SYSTEM_TIME AS OF a.proctime AS b\n" +
                "ON a.cityId = b.ID";
        /*
INSERT INTO order_detail
SELECT
    a.orderId,
    a.cityId,
    b.Name As cityName,
    a.goodsId,
    a.userId,
    a.price,
    a.createTime
FROM big_order As a
inner join city FOR SYSTEM_TIME AS OF a.proctime AS b
ON a.cityId = b.ID
         */


        bsTableEnv.executeSql(ddlMysqlSource);
        bsTableEnv.executeSql(ddlKafkaSource);
        bsTableEnv.executeSql(ddlSink);
//        bsTableEnv.executeSql(sql).print();
//        bsTableEnv.executeSql(sql1).print();
        Table tb = bsTableEnv.sqlQuery(sql2);
        DataStream<Row> dsRow = bsTableEnv.toAppendStream(tb, Row.class);
        dsRow.print();
        dsRow.map(r -> new OrderDetail(
                r.getField(0).toString(),
                Integer.parseInt(r.getField(1).toString()),
                r.getField(2).toString(),
                Integer.parseInt(r.getField(3).toString()),
                r.getField(4).toString(),
                Integer.parseInt(r.getField(5).toString()),
                0)
        ).addSink(new Sink2MySQLOrderDetail());

        bsEnv.execute("Blink Stream SQL demo ");
    }
}
