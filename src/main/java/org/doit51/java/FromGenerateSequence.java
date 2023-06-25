package org.doit51.java;

import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FromGenerateSequence {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> longDataStreamSource = executionEnvironment.generateSequence(1L, 5L);
//        executionEnvironment.setParallelism(3);
        //此处设置了并发度后，相当于转换成了新的算子，打印时要以新的算子去打印，否则，打印的内容是旧算子的内容
        DataStreamSource<Long> longDataStreamSource1 = longDataStreamSource.setParallelism(3);

        longDataStreamSource1.print();
        executionEnvironment.execute();

    }
}
