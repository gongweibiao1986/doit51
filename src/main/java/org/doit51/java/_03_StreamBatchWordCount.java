package org.doit51.java;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class _03_StreamBatchWordCount {
    public static void main(String[] args) throws Exception {
        //流处理的编程环境入口
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 按批计算模式去执行
//        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 按流计算模式去执行
         executionEnvironment.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // flink自己判断决定
//         executionEnvironment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        executionEnvironment.readTextFile("data/wc/input/").flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split("\\s+");
                for (String word : split) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(0).sum(1).print();
        executionEnvironment.execute("StreamingWordCount");
    }
}
