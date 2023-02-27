package vn.bnh.flink;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
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
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;

public class ConfluentAvroPubSubJob {
    public static final String JOB_NAME = ConfluentAvroPubSubJob.class.getName();
    public static String INPUT_TOPIC = null;
    public static String OUTPUT_TOPIC = null;
    private static String KAFKA_HOST = null;
    private static String SCHEMA_REGISTRY_URL = "http://schema-registry:8081";
    private static final StreamExecutionEnvironment FLINK_ENV = StreamExecutionEnvironment.getExecutionEnvironment();
    private static String SCHEMA_NAME = "flink-out-value";

    public static void initJob(String[] args) {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        INPUT_TOPIC = parameters.get("inputTopic", "flink-in");
        OUTPUT_TOPIC = parameters.get("outputTopic", "flink-out");
        KAFKA_HOST = parameters.get("broker", "broker:9092");
    }

    public static void executeJob() throws Exception {
//       set avro serializer class because default serializer use ImmutableCollection
        AvroKryoSerializerUtils avroKryoSerializerUtil = new AvroKryoSerializerUtils();
        avroKryoSerializerUtil.addAvroSerializersIfRequired(FLINK_ENV.getConfig(), GenericData.Record.class);
        FLINK_ENV.execute(JOB_NAME);

    }

    public static Schema getSchema() throws RestClientException, IOException {
        RestService restService = new RestService(SCHEMA_REGISTRY_URL);
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(restService, 100);
        Schema.Parser schemaParser = new Schema.Parser();
        return schemaParser.parse(schemaRegistryClient.getLatestSchemaMetadata(SCHEMA_NAME).getSchema());
    }


    public static KafkaSource initSource() {
        return KafkaSource.<String>builder().setBootstrapServers(KAFKA_HOST).setTopics(INPUT_TOPIC).setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class)).build();
    }

    public static KafkaSink initSink() throws RestClientException, IOException {
        return KafkaSink.<GenericRecord>builder().setBootstrapServers(KAFKA_HOST).setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic(OUTPUT_TOPIC).setValueSerializationSchema(ConfluentRegistryAvroSerializationSchema.forGeneric(SCHEMA_NAME, getSchema(), SCHEMA_REGISTRY_URL)).build()).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
    }

    public static void main(String[] args) throws Exception {
        // create execution environment
        initJob(args);
        KafkaSource source = initSource();
        KafkaSink sink = initSink();
        DataStreamSource<String> streamSource = FLINK_ENV.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<GenericRecord> outputStream = streamSource.map((MapFunction<String, GenericRecord>) value -> {
            GenericRecord genericRecord = new GenericData.Record(getSchema());
            genericRecord.put("Name", value);
            genericRecord.put("Job", JOB_NAME);
            return genericRecord;
        });
        outputStream.sinkTo(sink);
        executeJob();
    }
}
