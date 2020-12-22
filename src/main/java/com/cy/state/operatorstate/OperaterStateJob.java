package com.cy.state.operatorstate;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: chenye
 * @Date: 2020/2/28 18:34
 * @Blog:
 * @Description:
 */
public class OperaterStateJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /* 开启检查点机制 */
        env.enableCheckpointing(1000);
        // 设置并行度为1 或者 2 ，观察不同结果
        DataStreamSource<Tuple2<String, Long>> tuple2DataStreamSource = env.setParallelism(2).fromElements(
                Tuple2.of("a", 50L), Tuple2.of("a", 80L), Tuple2.of("a", 400L),
                Tuple2.of("a", 100L), Tuple2.of("a", 200L), Tuple2.of("a", 200L),
                Tuple2.of("b", 100L), Tuple2.of("b", 200L), Tuple2.of("b", 200L),
                Tuple2.of("b", 500L), Tuple2.of("b", 600L), Tuple2.of("b", 700L));
        tuple2DataStreamSource
                .flatMap(new ThresholdWarningRichFlatMap(100L, 3))
                .printToErr();
        env.execute("Managed Operater State");
    }

}
