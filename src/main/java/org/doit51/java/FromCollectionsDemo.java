package org.doit51.java;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FromCollectionsDemo {
    public static void main(String[] args) throws Exception {
        List<String> list = Arrays.asList("foo", "bar", "baz", "qux", "foo", "bar", "baz", "qux");
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.fromCollection(list);
        stringDataStreamSource.print();
        executionEnvironment.execute();


    }
}
