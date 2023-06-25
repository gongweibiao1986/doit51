package org.doit51.java02;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.scala.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 入门实例：通过socket流获取数据，并计算各个单词的数量
 */
public class _01_StreamWordCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",8081);
        //创建入口环境
        StreamExecutionEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        //模式设置
//        localEnvironment.getConfig().setExecutionMode(ExecutionMode.BATCH);

        //创建数据流
        DataStreamSource<String> stringDataStreamSource = localEnvironment.socketTextStream("192.168.80.181", 9999);
        //将获取到的数据分割后压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }


            }
        });
        //根据流中数据，统计单词数量

        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = tuple2SingleOutputStreamOperator.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        tuple2StringKeyedStream.sum("f1").print();
        localEnvironment.execute();
    }
}
