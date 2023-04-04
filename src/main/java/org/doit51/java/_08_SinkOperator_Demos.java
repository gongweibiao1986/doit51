package org.doit51.java;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.Map;


public class _08_SinkOperator_Demos {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/ckpt");
        env.setParallelism(2);

        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());
        // 把它打印: 输出到控制台
//        streamSource.print();
        //输出到文件(覆盖模式)
//        streamSource.writeAsText("d:/sink_test", FileSystem.WriteMode.OVERWRITE);
        streamSource.map(bean -> Tuple5.of(bean.getEventId(), bean.getGuid(), bean.getEventInfo(), bean.getSessionId(), bean.getTimeStamp())).returns(new TypeHint<Tuple5<String, Long, Map<String, String>, String, Long>>() {
        });
//                .writeAsCsv("d:/sink_test", FileSystem.WriteMode.OVERWRITE);

        env.execute("_08_SinkOperator_Demos");


    }
}


