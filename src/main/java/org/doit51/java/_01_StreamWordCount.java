package org.doit51.java;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _01_StreamWordCount {
    public static void main(String[] args) {
        //配置本地访问地址
        Configuration configuration= new Configuration();
        configuration.setInteger("rest.port",8081);

        LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment(configuration);

        DataStreamSource<String> stringDataStreamSource = localEnvironment.socketTextStream("192.168.80.181", 9999);



    }
}
