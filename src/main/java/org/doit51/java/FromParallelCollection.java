package org.doit51.java;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;
import org.apache.flink.util.SplittableIterator;

import java.util.Arrays;
import java.util.List;

public class FromParallelCollection {
    public static void main(String[] args) throws Exception {
        List<String> list = Arrays.asList("foo", "bar", "baz", "qux", "foo", "bar", "baz", "qux");
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(3);
        //fromParallelCollection(SplittableIterator, Class) 方法是一个并行的 Source（并行度可以使用 env 的
        //setParallelism 来设置），该方法需要传入两个参数，第一个是继承 SplittableIterator 的实现类的迭代器，第二个是迭代
        //器中数据的类型。
        DataStreamSource<Long> longDataStreamSource = executionEnvironment.fromParallelCollection(new NumberSequenceIterator(1L, 10L), Long.class);
        longDataStreamSource.print();
        executionEnvironment.execute();


    }
}
