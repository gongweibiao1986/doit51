package org.doit51.java01;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class _02_BatchWordCount {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8888);
        configuration.setInteger("taskmanager.numberOfTaskSlots", 16);

        LocalEnvironment localEnvironment = ExecutionEnvironment.createLocalEnvironment(configuration);
        System.out.println("localEnvironment 的并发数为"+localEnvironment.getParallelism());
        localEnvironment.setParallelism(2);
        System.out.println("设置并行度后，localEnvironment 的并发数为"+localEnvironment.getParallelism());
        DataSource<String> stringDataSource = localEnvironment.readTextFile("data/wc/input/wc.txt");
        System.out.println("stringDataSource 的并发数为"+stringDataSource.getParallelism());

        FlatMapOperator<String, Tuple2<String, Integer>> stringTuple2FlatMapOperator = stringDataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split("\\s+");
                for (String str : split) {
                    collector.collect(new Tuple2<>(str, 1));
                }
            }
        });
        System.out.println("stringTuple2FlatMapOperator 的并发数为"+stringTuple2FlatMapOperator.getParallelism());

        System.out.println(stringTuple2FlatMapOperator.getParallelism());
        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping = stringTuple2FlatMapOperator.groupBy(0);

        AggregateOperator<Tuple2<String, Integer>> sum = tuple2UnsortedGrouping.sum(1);
        sum.print();


    }
}
