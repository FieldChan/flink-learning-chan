package com.chan.state.operatorstate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: chenye
 * @Date: 2020/2/28 17:07
 * @Blog:
 * @Description: 算子状态 (Operator State)：顾名思义，状态是和算子进行绑定的，一个算子的状态不能被其他算子所访问到。
 * 官方文档上对 Operator State 的解释是：each operator state is bound to one parallel operator instance，
 * 所以更为确切的说一个算子状态是与一个并发的算子实例所绑定的，
 * 即假设算子的并行度是 2，那么其应有两个对应的算子状态
 */
public class ThresholdWarningRichFlatMap extends RichFlatMapFunction<Tuple2<String, Long>,
        Tuple2<String, List<Tuple2<String, Long>>>> implements CheckpointedFunction {

    // 非正常数据
    private List<Tuple2<String, Long>> bufferedData;
    // checkPointedState
    private transient ListState<Tuple2<String, Long>> checkPointedState;
    // 需要监控的阈值
    private Long threshold;
    // 次数
    private Integer numberOfTimes;

    ThresholdWarningRichFlatMap(Long threshold, Integer numberOfTimes) {
        this.threshold = threshold;
        this.numberOfTimes = numberOfTimes;
        this.bufferedData = new ArrayList<>();
    }

    //CheckpointedFunction 初始化state
    /**
     * This method is called when the parallel function instance is created during distributed
     * execution. Functions typically set up their state storing data structures in this method.
     *
     * @param context the context for initializing the operator
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 注意这里获取的是OperatorStateStore
        checkPointedState = context.getOperatorStateStore().
                getListState(new ListStateDescriptor<>("abnormalData",
                        TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                        })));
        // 如果发生重启，则需要从快照中将状态进行恢复
        if (context.isRestored()) {
            for (Tuple2<String, Long> element : checkPointedState.get()) {
                bufferedData.add(element);
            }
        }
    }

    @Override
    public void flatMap(Tuple2<String, Long> value,
                        Collector<Tuple2<String, List<Tuple2<String, Long>>>> out) {
        Long inputValue = value.f1;
        // 超过阈值则进行记录
        if (inputValue >= threshold) {
            bufferedData.add(value);
        }
        // 超过指定次数则输出报警信息
        if (bufferedData.size() >= numberOfTimes) {
            // 顺便输出状态实例的hashcode
            out.collect(Tuple2.of(checkPointedState.hashCode() + "阈值警报！", bufferedData));
            bufferedData.clear();
        }
    }

    //实现CheckpointedFunction 快照方法
    /**
     * This method is called when a snapshot for a checkpoint is requested. This acts as a hook to the function to
     * ensure that all state is exposed by means previously offered through {@link FunctionInitializationContext} when
     * the Function was initialized, or offered now by {@link FunctionSnapshotContext} itself.
     *
     * @param context the context for drawing a snapshot of the operator
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // 在进行快照时，将数据存储到checkPointedState
        checkPointedState.clear();
        for (Tuple2<String, Long> element : bufferedData) {
            checkPointedState.add(element);
        }
    }
}