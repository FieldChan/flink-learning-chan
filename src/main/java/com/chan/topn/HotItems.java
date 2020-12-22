package com.chan.topn;

import com.chan.topn.entity.UserBehavior;
import com.chan.topn.function.CountAggregateFunction;
import com.chan.topn.function.TopNKeyedProcessFunction;
import com.chan.topn.function.ResultWindowFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import java.net.URL;

/**
 * @Author: chenye
 * @Date: 2020/2/23 23:09
 * @Blog:
 * @Description: 本案例将实现一个“实时热门商品”的需求，我们可以将“实时热门商品”翻译成程序员更好理解的需求：每隔5分钟输出最近一小时内点击量最多的前 N 个商品。
 * 将这个需求进行分解我们大概要做这么几件事情：
 * 抽取出业务时间戳，告诉 Flink 框架基于业务时间做窗口
 * 过滤出点击行为数据
 * 按一小时的窗口大小，每5分钟统计一次，做滑动窗口聚合（Sliding Window）
 * 按每个窗口聚合，输出每个窗口中点击量前N名的商品
 */
public class HotItems {


    public static void main(String[] args) throws Exception {

        // 创建 execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 告诉系统按照 EventTime 处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 为了打印到控制台的结果不乱序，我们配置全局的并发为1，改变并发对结果正确性没有影响
        env.setParallelism(1);

        // UserBehavior.csv 的本地文件路径, 在 resources 目录下
        URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
        // 抽取 UserBehavior 的 TypeInformation，是一个 PojoTypeInfo
        PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
        // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
        String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        // 创建 PojoCsvInputFormat
        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);


        env
            // 创建数据源，得到 UserBehavior 类型的 DataStream
            .createInput(csvInput, pojoType)
            // 抽取出时间和生成 watermark
            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                @Override
                public long extractAscendingTimestamp(UserBehavior userBehavior) {
                    // 原始数据单位秒，将其转成毫秒
                    return userBehavior.timestamp * 1000;
                }
            })
            // 过滤出只有点击的数据
            .filter(new FilterFunction<UserBehavior>() {
                @Override
                public boolean filter(UserBehavior userBehavior) throws Exception {
                    // 过滤出只有点击的数据
                    return userBehavior.behavior.equals("pv");
                }
            })
            .keyBy("itemId")
            .timeWindow(Time.minutes(60), Time.minutes(5))
            .aggregate(new CountAggregateFunction(), new ResultWindowFunction())
                .keyBy("windowEnd")
                /**求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串*/ /** 用于输出窗口的结果 */
                .process(new TopNKeyedProcessFunction(3))
            .print();

        env.execute("Hot Items Job");
    }


}
