package vn.bnh.flink;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.StringDeserializer;

public class PublishGenericAvroJob {
    public static Schema getSchema() {
        String schemaString = "{\n" +
                "   \"type\" : \"record\",\n" +
                "   \"namespace\" : \"Tutorialspoint\",\n" +
                "   \"name\" : \"Employee\",\n" +
                "   \"fields\" : [\n" +
                "      { \"name\" : \"Name\" , \"type\" : \"string\" },\n" +
                "      { \"name\" : \"Job\" , \"type\" : \"string\" }\n" +
                "   ]\n" +
                "}\n";
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }

    public static final String JOB_NAME = "PublishAvroJob";

    public static void main(String[] args) throws Exception {

        ParameterTool parameters = ParameterTool.fromArgs(args);
        String inputTopic = parameters.get("inputTopic", "flink-in");
        String outputTopic = parameters.get("outputTopic", "flink-out");
        String kafkaHost = parameters.get("broker", "broker:9092");
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//       set avro serializer class because default serializer use ImmutableCollection
        AvroKryoSerializerUtils avroKryoSerializerUtil = new AvroKryoSerializerUtils();
        avroKryoSerializerUtil.addAvroSerializersIfRequired(env.getConfig(), GenericData.Record.class);

        Schema schema = getSchema();

        KafkaSource<String> sourceBuilder = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaHost)
                .setTopics(inputTopic)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .build();
        AvroSerializationSchema<GenericRecord> serializationSchema = AvroSerializationSchema.forGeneric(schema);
        KafkaSink<GenericRecord> sink = KafkaSink.<GenericRecord>builder()
                .setBootstrapServers(kafkaHost)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(serializationSchema)
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
//        subscribe to source
        DataStreamSource<String> streamSource = env.fromSource(sourceBuilder, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        transformation
        SingleOutputStreamOperator<GenericRecord> outputStream = streamSource.map((MapFunction<String, GenericRecord>) value -> {
            GenericRecord genericRecord = new GenericData.Record(schema);
            genericRecord.put("Name", value);
            genericRecord.put("Job", JOB_NAME);
            return genericRecord;
        });
//        produce to target
        outputStream.sinkTo(sink);
        env.execute(JOB_NAME);

    }
}
