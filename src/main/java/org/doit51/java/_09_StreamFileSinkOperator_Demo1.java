package org.doit51.java;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;

public class _09_StreamFileSinkOperator_Demo1 {
    public static void main(String[] args) {
/**
 * 应用  StreamFileSink 算子，来将数据输出到  文件系统
 */

        /**
         * 1. 输出为 行格式
         */
//        //构建一个FileSink对象
//        FileSink<String> rowSize = FileSink.<String>forRowFormat(new Path("d:/filesink/"), new SimpleStringEncoder<>("utf-8"))
//                //文件的滚动策略（间隔时间10s,或文件大小达到5m,就进行文件切换）
//                .withRollingPolicy(DefaultRollingPolicy.builder().withRolloverInterval(10000).withMaxPartSize(5 * 1024 * 1024).build())
//                //分桶的策略（划分子文件夹的策略）
//                .withBucketAssigner(new DateTimeBucketAssigner<>())
//                .withBucketCheckInterval(5)
//                //输出文件的文件名相关配置
//                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("doitedu").withPartSuffix(".txt").build())
//                .build();
//        //然后添加到流，进行输出
//        streamSource.map(JSON::toJSONString)
//                .addSink()/*SinkFunction实现类对象，用addSink()来添加*/
//                .sinkTo(rowSize);/*Sink 的实现类对象，用sinkTo()来添加*/
        /**
         //         * 2. 输出为 列格式
         //         *
         //         * 要构造一个列模式的 FileSink，需要一个ParquetAvroWriterFactory
         //         * 而获得ParquetAvroWriterFactory的方式是，利用一个工具类： ParquetAvroWriters
         //         * 这个工具类提供了3种方法，来为用户构造一个ParquetAvroWriterFactory
         //         *
         //         * ## 方法1：
         //         * writerFactory = ParquetAvroWriters.forGenericRecord(schema)
         //         *
         //         * ## 方法2：
         //         * writerFactory = ParquetAvroWriters.forSpecificRecord(AvroEventLogBean.class)
         //         *
         //         * ## 方法3：
         //         * writerFactory = ParquetAvroWriters.forReflectRecord(EventLog.class);
         //         *
         //         * 一句话：  3种方式需要传入的参数类型不同
         //         * 1. 需要传入schema对象
         //         * 2. 需要传入一种特定的JavaBean类class
         //         * 3. 需要传入一个普通的JavaBean类class
         //         *
         //         * 传入这些参数，有何用？
         //         * 这个工具 ParquetAvroWriters. 需要你提供你的数据模式schema（你要生成的parquet文件中数据模式schema）
         //         *
         //         * 上述的3种参数，都能让这个工具明白你所指定的数据模式（schema）
         //         *
         //         * 1. 传入Schema类对象，它本身就是parquet框架中用来表达数据模式的内生对象
         //         *
         //         * 2. 传入特定JavaBean类class，它就能通过调用传入的类上的特定方法，来获得Schema对象
         //         * （这种特定JavaBean类，不用开发人员自己去写，而是用avro的schema描述文件+代码生产插件，来自动生成）
         //         *
         //         * 3. 传入普通JavaBean,然后工具可以自己通过反射手段来获取用户的普通JavaBean中的包名、类名、字段名、字段类型等信息，来翻译成一个符合Avro要求的Schema
         //         *
         //         *
         //         */
//        // ### 方式 1： 手动构造schema，来生成ParquetAvroWriter工厂
        Schema scheme = Schema.createRecord("id","用户id","org.doit51.exercise.UserInfo",true);
        ParquetWriterFactory<GenericRecord> writerFactory0  = ParquetAvroWriters.forGenericRecord(scheme);// 根据调用的方法及传入的信息，来获取avro模式schema，并生成对应的parquetWriter

    }
}
