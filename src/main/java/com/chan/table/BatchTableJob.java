package com.chan.table;

import com.chan.common.entity.PlayerData;
import com.chan.common.entity.PlayerResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;

/**
 * @Author: chenye
 * @Date: 2020/2/29 13:42
 * @Blog:
 * @Description:
 *
 */
public class BatchTableJob {
    public static void main(String[] args) throws Exception {

        /**
        // 对于批处理程序，使用ExecutionEnvironment而不是StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建一个TableEnvironment
        // 对于批处理程序使用BatchTableEnvironment而不是StreamTableEnvironment
                StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 注册一个 Table
                tableEnv.registerTable("table1", ...)            // 或者
                tableEnv.registerTableSource("table2", ...);     // 或者
                tableEnv.registerExternalCatalog("extCat", ...);
        // 注册一个输出 Table
                tableEnv.registerTableSink("outputTable", ...);

        / 从 Table API query 创建一个Table
                Table tapiResult = tableEnv.scan("table1").select(...);
        // 从 SQL query 创建一个Table
                Table sqlResult  = tableEnv.sqlQuery("SELECT ... FROM table2 ... ");

        // 将表API结果表发送到TableSink，对于SQL结果也是如此
                tapiResult.insertInto("outputTable");

        // 执行
                env.execute();
         **/


        //1\. 获取上下文环境 table的环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        //2\. 读取score.csv
        DataSet<String> input = env.readTextFile("E:\\CyEcommerce\\src\\main\\resources\\score.csv");
        input.print();

        DataSet<PlayerData> topInput = input.map(new MapFunction<String, PlayerData>() {
            @Override
            public PlayerData map(String s) throws Exception {
                String[] split = s.split(",");

                return new PlayerData(String.valueOf(split[0]),
                        String.valueOf(split[1]),
                        String.valueOf(split[2]),
                        Integer.valueOf(split[3]),
                        Double.valueOf(split[4]),
                        Double.valueOf(split[5]),
                        Double.valueOf(split[6]),
                        Double.valueOf(split[7]),
                        Double.valueOf(split[8])
                );
            }
        });

        //3\. 注册成内存表
        Table topScore = tableEnv.fromDataSet(topInput);
        tableEnv.registerTable("score", topScore);

        //4\. 编写sql 然后提交执行
        //select player, count(season) as num from score group by player order by num desc;
        Table queryResult = tableEnv.sqlQuery("select player, count(season) as num from score group by player order by num desc limit 3");

        //5\. 结果进行打印
        DataSet<PlayerResult> result = tableEnv.toDataSet(queryResult, PlayerResult.class);
        result.print();

        //当然我们也可以自定义一个 Sink，将结果输出到一个文件中，例如：
        TableSink sink = new CsvTableSink("E:\\CyEcommerce\\src\\main\\resources\\playerresult.csv", ",");
        String[] fieldNames = {"player", "num"};
        TypeInformation[] fieldTypes = {Types.STRING, Types.LONG};
        tableEnv.registerTableSink("playerresult", fieldNames, fieldTypes, sink);
        queryResult.insertInto("playerresult");
        env.execute();



    }
}
