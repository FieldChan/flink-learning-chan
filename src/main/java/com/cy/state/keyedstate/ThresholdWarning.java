package com.cy.state.keyedstate;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
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
public class ThresholdWarning extends
        RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, List<Long>>> {

    // 通过ListState来存储非正常数据的状态
    private transient ListState<Long> abnormalData;
    // 需要监控的阈值
    private Long threshold;
    // 触发报警的次数
    private Integer numberOfTimes;

    ThresholdWarning(Long threshold, Integer numberOfTimes) {
        this.threshold = threshold;
        this.numberOfTimes = numberOfTimes;
    }

    @Override
    public void open(Configuration parameters) {
        // 通过状态名称(句柄)获取状态实例，如果不存在则会自动创建
        abnormalData = getRuntimeContext().getListState(
                new ListStateDescriptor<>("abnormalData", Long.class));
    }

    @Override
    public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, List<Long>>> out)
            throws Exception {
        Long inputValue = value.f1;
        // 如果输入值超过阈值，则记录该次不正常的数据信息
        if (inputValue >= threshold) {
            abnormalData.add(inputValue);
        }
        ArrayList<Long> list = Lists.newArrayList(abnormalData.get().iterator());
        // 如果不正常的数据出现达到一定次数，则输出报警信息
        if (list.size() >= numberOfTimes) {
            out.collect(Tuple2.of(value.f0 + " 超过指定阈值 ", list));
            // 报警信息输出后，清空状态
            abnormalData.clear();
        }
    }
}