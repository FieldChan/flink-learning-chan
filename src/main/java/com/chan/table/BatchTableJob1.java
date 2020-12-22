package com.chan.table;

import com.chan.common.entity.PlayerResult1;
import com.chan.common.entity.TopScorers;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

/**
 * @Author: chenye
 * @Date: 2020/2/29 15:54
 * @Blog:
 * @Description:
 */

public class BatchTableJob1 {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        //source,这里读取CSV文件，并转换为对应的Class
        DataSet<TopScorers> csvInput = env
                .readCsvFile("E://flink-training-exercises//src//main//resource//2016_Chinese_Super_League_Top_Scorers.csv")
                .ignoreFirstLine() .pojoType(TopScorers.class,"rank","player","country","club","total_score","total_score_home","total_score_visit","point_kick");

        //将DataSet转换为Table
        Table topScore = tableEnv.fromDataSet(csvInput);
        //将topScore注册为一个表
        tableEnv.registerTable("topScore",topScore);
        //查询球员所在的国家，以及这些国家的球员（内援和外援）的总进球数
        Table groupedByCountry = tableEnv.sqlQuery("select country,sum(total_score) as sum_total_score from topScore group by country order by 2 desc");
        //转换回dataset
        DataSet<PlayerResult1> result = tableEnv.toDataSet(groupedByCountry,PlayerResult1.class);

        //将dataset map成tuple输出
        result.map(new MapFunction<PlayerResult1, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(PlayerResult1 result) throws Exception {
                String country = result.country;
                int sum_total_score = result.sum_total_score;
                return Tuple2.of(country,sum_total_score);
            }
        }).print();

    }


}
