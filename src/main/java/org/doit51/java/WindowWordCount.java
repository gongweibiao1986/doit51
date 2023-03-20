package org.doit51.java;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowWordCount {
    public static void main(String[] args) throws Exception {

        // 显式声明为本地运行环境，且带webUI
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> socketTextStream = env.socketTextStream("192.168.80.181", 9999);
        System.out.println("env的并发度是" + env.getParallelism());
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String str : s1) {
                    collector.collect(new Tuple2<>(str, 1));
                }
            }
        });
        System.out.println("tuple2SingleOutputStreamOperator的并发度是" + tuple2SingleOutputStreamOperator.getParallelism());
        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = tuple2SingleOutputStreamOperator.keyBy(value -> value.f0);

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = tuple2StringKeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
        window.sum(1).print();
        env.execute("Window WordCount");

    }
}