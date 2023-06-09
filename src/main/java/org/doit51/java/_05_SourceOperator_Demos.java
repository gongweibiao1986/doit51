package org.doit51.java;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.LongValueSequenceIterator;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class _05_SourceOperator_Demos {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8089);
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(configuration);
        env.setParallelism(3);

        /**
         * 从集合中获取数据流,底层调用的是fromCollection
         */

        DataStreamSource<Integer> fromElements = env.fromElements(1, 2, 3, 4, 5);

//        fromElements.map(d -> d * 10).print();

        List<String> dataList = Arrays.asList("a", "b", "a", "c");
        //fromCollection方法所返回的source算子，是一个单并行度的source算子
        DataStreamSource<String> formCollection = env.fromCollection(dataList);
//        formCollection.map(value -> value.toUpperCase()).print();
        /**
         *fromParallelCollection所返回的source算子，是一个多并行度的source算子
         */
        DataStreamSource<LongValue> parallelCollection = env.fromParallelCollection(new LongValueSequenceIterator(1, 100), TypeInformation.of(LongValue.class)).setParallelism(2);
//        parallelCollection.map(lv -> lv.getValue() + 100).print();

        DataStreamSource<Long> sequence = env.generateSequence(1, 100);
        sequence.map(x -> x - 1).print();

        /**
         * 从socket端口获取数据得到数据流
         *socketTextStream方法产生的source算子，是一个单并行度的source算子
         */
//        env.socketTextStream("localhost",9999);

        /**
         * 基于文件的 Source，本质上就是使用指定的 FileInputFormat 组件读取数据，可以指定 TextInputFormat、
         * CsvInputFormat、BinaryInputFormat 等格式；
         * 底层都是 ContinuousFileMonitoringFunction，这个类继承了 RichSourceFunction，都是非并行的 Source；
         */

        /**
         * 从文件得到数据流
         * readTextFile(String filePath) 可以从指定的目录或文件读取数据，默认使用的是 TextInputFormat 格式
         * 读取数据，还有一个重载的方法 readTextFile(String filePath, String charsetName)可以传入读取文件指定
         * 的字符集，默认是 UTF-8 编码。该方法是一个有限的数据源，数据读完后，程序就会退出，不能一
         * 直运行。该方法底层调用的是 readFile 方法，FileProcessingMode 为 PROCESS_ONCE
         */
        DataStreamSource<String> fileSource = env.readTextFile("data/wc/input", "utf-8");
//        fileSource.map(value -> value.toUpperCase()).print();

        //readFile(FileInputFormat inputFormat, String filePath) 方法可以指定读取文件的 FileInputFormat 格式，
        //参数 FileProcessingMode，可取值：
        // PROCESS_ONCE，只读取文件中的数据一次，读取完成后，程序退出
        // PROCESS_CONTINUOUSLY，会一直监听指定的文件，文件的内容发生变化后，会将以前的内容和新的内容全
        //部都读取出来，进而造成数据重复读取。

        //PROCESS_CONTINUOUSLY 模式是一直监听指定的文件或目录，2 秒钟检测一次文件是否发生变化
        DataStreamSource<String> fileSource2 = env.readFile(new TextInputFormat(null), "data/wc/input", FileProcessingMode.PROCESS_CONTINUOUSLY, 1000);
//        fileSource2.map(String::toUpperCase).print();


        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                //设置主题
                .setTopics("test")
                //设置组id
                .setGroupId("gp01")
                //kafka服务器地址
                .setBootstrapServers("192.168.80.181:9092,192.168.80.182:9092,192.168.80.183:9092")


                //起始消费位移指定
                /////////消费起始唯一选择之前提交的位移量（如果没有，则充值为LATEST）
//               OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST)
                /////////消费起始位移直接选择为 “最早”
//               OffsetsInitializer.earliest()
                /////////消费起始位移直接选择为 “最新”
//               OffsetsInitializer.latest()
                /////////消费起始位移选择为：方法所传入的每个分区和对应的起始偏移量
//        OffsetsInitializer.offsets(Map < TopicPartition, Long >)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                //设置value数据的反序列化器
                .setValueOnlyDeserializer(new SimpleStringSchema())

                //开启kafka底层消费者的自动位移提交机制
                //    它会把最新的消费位移提交到kafka的consumer_offsets中
                //    就算把自动位移提交机制开启，KafkaSource依然不依赖自动位移提交机制
                //    （宕机重启时，优先从flink自己的状态中去获取偏移量<更可靠>）
                .setProperty("auto.offset.commit", "true")
                // 把本source算子设置成  BOUNDED属性（有界流）
                //     将来本source去读取数据的时候，读到指定的位置，就停止读取并退出
                //     常用于补数或者重跑某一段历史数据
                //                     .setBounded(OffsetsInitializer.committedOffsets())
                // 把本source算子设置成  UNBOUNDED属性（无界流）
                //     但是并不会一直读数据，而是达到指定位置就停止读取，但程序不退出
                //     主要应用场景：需要从kafka中读取某一段固定长度的数据，然后拿着这段数据去跟另外一个真正的无界流联合处理
                //.setUnbounded(OffsetsInitializer.latest())
                .build();

        /**
         *
         */
        // env.addSource();  //  接收的是  SourceFunction接口的 实现类

//        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KFK-source").print();

        env.execute("SourceOpertion");
    }
}
