package vn.bnh.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ModifyKafkaMessageJob {
    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String inputTopic = parameters.get("inputTopic", "transactions");
        String outputTopic = parameters.get("outputTopic", "fraud");
        String kafkaHost = parameters.get("broker", "kafka:9092");
        String jobName = parameters.get("jobName", "Sample Kafka Stream Job");
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        KafkaSource<String> sourceBuilder = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaHost)
                .setTopics(inputTopic)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .build();
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaHost)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        DataStreamSource<String> streamSource = env.fromSource(sourceBuilder, WatermarkStrategy.noWatermarks(), "Kafka Source");
        streamSource.map((MapFunction<String, String>) value -> value);
        streamSource.print();
        SingleOutputStreamOperator<String> outputStream = streamSource.map((MapFunction<String, String>) value -> "modified value:" + value);
        outputStream.sinkTo(sink);
        env.execute(jobName);
    }
}
