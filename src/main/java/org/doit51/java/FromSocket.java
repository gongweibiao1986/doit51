package org.doit51.java;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FromSocket {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //该方法还有多个重载的方法，如：
        //socketTextStream(String hostname, int port, String delimiter, long maxRetry)
        //可以指定行分隔符和最大重新连接次数。

        //提示：socketSource 是一个非并行 source，如果使用 socketTextStream 读取数据，在启动 Flink 程序之前，必须先
        //启动一个 Socket 服务，为了方便，Mac 或 Linux 用户可以在命令行终端输入 nc -lk 8888 启动一个 Socket 服务并在命令
        //行中向该 Socket 服务发送数据。
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.socketTextStream("192.168.80.181", 9999);
        stringDataStreamSource.print();
        executionEnvironment.execute();

    }
}
