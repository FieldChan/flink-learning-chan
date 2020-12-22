package com.chan.common.utils;


import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author
 * @time 2020-01-04 15:26:14
 */
public class CheckpointUtil {

    private static final boolean ENABLE_INCREMENTAL_CHECKPOINT = true;
    private static final int NUMBER_OF_TRANSFER_THREADS = 3;

    /**
     * 设置状态后端为 RocksDBStateBackend
     *
     * @param env env
     * @throws IOException
     */
    public static void setRocksDBStateBackend(StreamExecutionEnvironment env) throws IOException {

        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(10));

        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        checkpointConf.setMinPauseBetweenCheckpoints(TimeUnit.MINUTES.toMillis(8));
        checkpointConf.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(15));
        checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(
                "hdfs:///user/flink/checkpoints", ENABLE_INCREMENTAL_CHECKPOINT);
        rocksDBStateBackend.setNumberOfTransferThreads(NUMBER_OF_TRANSFER_THREADS);
        rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
//        rocksDBStateBackend.enableTtlCompactionFilter(); // enabled by default
        env.setStateBackend((StateBackend) rocksDBStateBackend);
    }


    /**RocksDBStateBackend
     * 设置状态后端为 FsStateBackend
     *
     * @param env env
     * @throws IOException
     */
    public static void setFsStateBackend(StreamExecutionEnvironment env) throws IOException {
        setConfYamlStateBackend(env);

        FsStateBackend fsStateBackend = new FsStateBackend("file:\\D:\\tmp\\flinkcheckpoints");//  hdfs:///user/flink/checkpoints
        env.setStateBackend((StateBackend) fsStateBackend);
    }


    /**
     * 设置状态后端为 MemoryStateBackend
     *
     * @param env env
     * @throws IOException
     */
    public static void setMemoryStateBackend(StreamExecutionEnvironment env) throws IOException {
        setConfYamlStateBackend(env);
        env.setStateBackend((StateBackend) new MemoryStateBackend());
    }

    /**
     * Checkpoint 参数相关配置，but 不设置 StateBackend，即：读取 flink-conf.yaml 文件的配置
     *
     * @param env env
     * @throws IOException
     */
    public static void setConfYamlStateBackend(StreamExecutionEnvironment env) throws IOException {
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(60));

        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        checkpointConf.setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(10));
        checkpointConf.setCheckpointTimeout(TimeUnit.SECONDS.toMillis(30));
        checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

    }

    public static void setCheckpoint(StreamTableEnvironment tEnv) {
        tEnv.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        tEnv.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));
        tEnv.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT, CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        tEnv.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, 1); //默认值为1
        tEnv.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofMinutes(20)); //默认值为10min

    }
}
