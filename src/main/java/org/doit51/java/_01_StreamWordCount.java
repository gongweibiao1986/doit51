package org.doit51.java;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class _01_StreamWordCount {
    public static void main(String[] args) throws Exception {
        //配置本地访问地址
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);

        //创建本地环境，flink是流批一体的，可以通过设置运行mode来设置运行模式

        LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment(configuration);
//        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();//批处理环境
//        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);//流处理环境
//        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.BATCH);//批运行模式
//        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.STREAMING);//流运行模式
//        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);//自行判断运行模式

        /**
         * 本地运行模式时，程序的默认并行度为 ，你的cpu的逻辑核数
         */
//        executionEnvironment.setParallelism(2); // 默认并行度可以通过env人为指定

        // 通过source算子，把socket数据源加载为一个dataStream（数据流）
        // [root@doit01 ~]# nc -lk 9999
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();
        DataStreamSource<String> stringDataStreamSource = localEnvironment.socketTextStream("192.168.241.128", 9999);

//        stringDataStreamSource
//                .setParallelism(1)
//                .slotSharingGroup("g1");

        stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] s1 = s.split(" ");
                        for (String word : s1) {
                            collector.collect(Tuple2.of(word, 1));
                        }
                    }
                })
//                .slotSharingGroup("g2")
                .shuffle()
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                })
//                .keyBy(value -> value.f0)//简单写法
                .sum("f1")
//                .print("my job");
                .print();

//flink任务状态一直是created状态,
// yarn slot 满了，减少并发度
// 或者单个cpu使用率高，增加并发度， 减少单个并发内存


        localEnvironment.execute("job");

    }
}
