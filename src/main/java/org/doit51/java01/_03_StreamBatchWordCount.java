package org.doit51.java01;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class _03_StreamBatchWordCount {
    public static void main(String[] args) throws Exception {
        // 程序入口
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println("executionEnvironment 的并发数为"+executionEnvironment.getParallelism());
        executionEnvironment.setParallelism(2);
        System.out.println("设置并行度后，executionEnvironment 的并发数为"+executionEnvironment.getParallelism());
        //设置为批执行模式
        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //设置流执行模式
//        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        //设置为自动判定执行模式
//        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //读文件到Stream
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("data/wc/input/wc.txt");
        System.out.println("stringDataStreamSource 的并发数为"+stringDataStreamSource.getParallelism());
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split("\\s+");
                for (String str : split) {
                    collector.collect(new Tuple2<String, Integer>(str, 1));
                }

            }
        });
        System.out.println("tuple2SingleOutputStreamOperator 的并发数为"+tuple2SingleOutputStreamOperator.getParallelism());
        tuple2SingleOutputStreamOperator .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        }).sum(1).print();

        executionEnvironment.execute("StreamBatchTest");
    }
}
