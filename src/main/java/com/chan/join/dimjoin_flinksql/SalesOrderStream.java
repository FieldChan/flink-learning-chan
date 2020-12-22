package com.chan.join.dimjoin_flinksql;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.flink.table.api.Expressions.*;


public class SalesOrderStream {

    private static Logger log = LoggerFactory.getLogger(SalesOrderStream.class.getName());
    public static Table report(Table transactions) {

        return transactions.select(
                $("customer_name"),
                $("created_date"),
                $("total_amount"))
                .groupBy($("customer_name"),$("created_date"))
                .select(
                        $("customer_name"),
                        $("total_amount").sum().as("total_amount"),
                        $("created_date")
                );

    }

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
//        env.setParallelism(4);
//        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // set default parallelism to 4


//        tEnv.executeSql("CREATE TABLE sales_order_header_stream (\n" +
////                "   id  BIGINT not null,\n" +
//                "    customer_name STRING,\n"+
////                "    dsp_org_name STRING,\n"+
//                "    total_amount      DECIMAL(38,2),\n" +
////                "    total_discount      DECIMAL(16,2),\n" +
////                "    pay_amount      DECIMAL(16,2),\n" +
////                "    total_amount      DECIMAL(16,2),\n" +
//                "    created_date TIMESTAMP(3)\n" +
//                ") WITH (\n" +
//                " 'connector' = 'mysql-cdc',\n" +
//                " 'hostname' = '192.168.8.73',\n" +
//                " 'port' = '4000',\n"+
//                " 'username' = 'flink',\n"+
//                " 'password' = 'flink',\n"+
//                " 'database-name' = 'dspdev',\n"+
//                " 'table-name' = 'sales_order_header'\n"+
//                ")");
        //pay_type,over_sell
        tEnv.executeSql("CREATE TABLE sales_order_header_stream (\n" +
                " `id` BIGINT,\n"+
                " `total_amount` DECIMAL(16,2) ,\n"+
                " `customer_name` STRING,\n"+
                " `order_no` STRING,\n"+
                " `doc_type` STRING,\n"+
                " `sales_org` STRING,\n"+
                " `distr_chan` STRING,\n"+
                " `division` STRING,\n"+
                " `sales_grp` STRING,\n"+
                " `sales_off` STRING,\n"+
                " `purch_no_c` STRING,\n"+
                " `purch_date` STRING,\n"+
                " `sold_to` STRING,\n"+
                " `ship_to` STRING,\n"+
                " `r3_sales_order` STRING,\n"+
                " `created_by_employee_name` STRING,\n"+
                " `created_by_dept_name` STRING,\n"+
                " `created_by_dept_name` STRING,\n"+
                " `is_enable` BIGINT,\n"+
                " `is_delete` BIGINT,\n"+
                " `sale_order_status` STRING,\n"+
                " `created_by_parent_dept_name` STRING,\n"+
                " `total_discount` DECIMAL(16,2),\n"+
                " `customer_sapcode` STRING,\n"+
                " `sold_to_name` STRING,\n"+
                " `ship_to_name` STRING,\n"+
                " `total_discount_amount` DECIMAL(16,2),\n"+
                " `other_discount` DECIMAL(16,2),\n"+
                " `other_amount` DECIMAL(16,2),\n"+
                " `pay_amount` DECIMAL(16,2),\n"+
                " `dsp_org_name` STRING,\n"+
                " `delivery_address` STRING,\n"+
                " `delivery_person` STRING,\n"+
                " `delivery_phone` STRING,\n"+
                " `pay_type` STRING,\n"+
                " `over_sell` STRING,\n"+
                " `created_date` TIMESTAMP(3),\n"+
                " PRIMARY KEY (`id`) NOT ENFORCED "+
                ") WITH (\n" +
                "'connector' = 'kafka',\n"+
                "'topic' = 'canal-data',\n"+
                "'properties.bootstrap.servers' = '192.168.8.71:9092',\n"+
                "'properties.group.id' = 'test',\n"+
                "'format' = 'canal-json'\n"+
                ")");

//        tEnv.executeSql("CREATE TABLE total_day_report (\n" +
//                "    customer_name STRING,\n" +
////                "    total_amount    DECIMAL(16,2),\n" +
////                "    total_discount  DECIMAL(16,2),\n" +
////                "    pay_amount      DECIMAL(16,2),\n" +
//                "    total_amount    DECIMAL(16,2),\n" +
//                "    created_date STRING,\n" +
//                "    PRIMARY KEY (created_date) NOT ENFORCED" +
//                ") WITH (\n" +
//                "  'connector' = 'upsert-kafka',\n" +
//                "  'topic' = 'customer_amount',\n" +
//                "  'properties.bootstrap.servers' = '192.168.8.71:9092',\n"+
//                "  'key.format' = 'json',\n"+
//                "  'value.format' = 'json',\n"+
//                "  'value.fields-include' = 'ALL'\n"+
//                ")");



        tEnv.executeSql("CREATE TABLE upsertSink (\n" +
                "    customer_name STRING,\n" +
//                "    total_amount    DECIMAL(16,2),\n" +
//                "    total_discount  DECIMAL(16,2),\n" +
//                "    pay_amount      DECIMAL(16,2),\n" +
                "    total_amount    DECIMAL(16,2),\n" +
                "    created_date STRING,\n" +
                "    PRIMARY KEY (customer_name,created_date) NOT ENFORCED" +
                ") WITH (\n" +
                "  'connector' = 'tidb',\n" +
                "  'tidb.database.url' = 'jdbc:mysql://192.168.8.73:4000/dspdev',\n" +
                "  'tidb.username' = 'flink',\n"+
                "  'tidb.password' = 'flink',\n"+
                "  'tidb.database.name' = 'dspdev',\n"+
                "  'tidb.table.name' = 'spend_report'\n"+
//                "  'connector.type'='jdbc'," +
//                "  'connector.url'='jdbc:mysql://192.168.8.73:4000/dspdev',\n" +
//                "  'connector.username' = 'flink',\n"+
//                "  'connector.password' = 'flink',\n"+
//                "  'connector.table' = 'spend_report'" +
                ")");
//        TableSchema tableSche = TableSchema.builder()
//                .field("customer_name",DataTypes.STRING().notNull())
//                .field("total_amount",DataTypes.DECIMAL(16,2))
//                .field("created_date",DataTypes.STRING().notNull()).build();
//
//        JdbcOptions jdbcOptions = JdbcOptions.builder()
//                .setDBUrl("jdbc:mysql://192.168.8.73:4000/dspdev")
//                .setTableName("spend_report")
//                .setUsername("flink")
//                .setPassword("flink")
//                .setDialect(new MySQLDialect())
//                .setDriverName("com.mysql.jdbc.Driver")
//                .build();
//        JdbcUpsertTableSink jdbcUpsertTableSink = JdbcUpsertTableSink.builder()
//                .setTableSchema(tableSche)
//                .setOptions(jdbcOptions)
//                .build();
//        jdbcUpsertTableSink.setKeyFields(new String[]{"id"});
        /**
         * SINK End
         */
//        tEnv.re("spend_report",jdbcUpsertTableSink);
        Table transactions = tEnv.from("sales_order_header_stream");
//      tEnv.executeSql("delete from total_day_report");
        tEnv.executeSql("insert into upsertSink select dsp_org_name as customer_name,cast(sum(t.pay_amount) as decimal(16,2)) as amount,DATE_FORMAT(t.created_date,'yyyy-MM-dd') as created_date from sales_order_header_stream t group by DATE_FORMAT(t.created_date,'yyyy-MM-dd'),dsp_org_name").print();
//      tEnv.executeSql("insert into spend_report select * from total_day_report");
//      Table transactions = tEnv.from("total_day_report");
//      report(transactions).executeInsert("spend_report");
        tEnv.execute("-----------");
    }


}
