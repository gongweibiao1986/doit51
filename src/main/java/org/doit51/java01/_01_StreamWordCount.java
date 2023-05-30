package org.doit51.java01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class _01_StreamWordCount {

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8080);
        //设置可用的slot数目为8个，不设置，默认为本机cpu数量
        configuration.setInteger("taskmanager.numberOfTaskSlots", 8);

        LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment(configuration);
        DataStreamSource<String> socketTextStream = localEnvironment.socketTextStream("192.168.241.128", 9999);
        socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String str : s1) {
                    collector.collect(new Tuple2<String, Integer>(str, 1));
                }

            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        }).sum("f1").print();


    }
}
