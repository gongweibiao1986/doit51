package org.doit51.java;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import scala.tools.util.PathResolver;

import java.io.File;

public class _02_BatchWordCount {
    public static void main(String[] args) throws Exception {
        //设置配置文件
        Configuration configuration = new Configuration();
        //配置web ui的本地访问地址
        configuration.setInteger("rest.port", 8081);
        //配置slot的数量，默认是当前机器核数
        configuration.setInteger("taskmanager.numberOfTaskSlots", 8);
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        // 读数据  -- : 批计算中得到的数据抽象，是一个 DataSet
        DataSource<String> stringDataSource = executionEnvironment.readTextFile("data/wc/input");
        File file = new File(".");
        System.out.println(file.getAbsoluteFile());
        System.out.println(file.getName());

        stringDataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split("\\s+");
                for (String word : split) {
                    out.collect(Tuple2.of(word, 1));
                }

            }
        }).groupBy(0).sum(1).print();
        //在批模式中，可以不进行env.execute


    }
}
