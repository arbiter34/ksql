package io.confluent.ksql.serde.protobuf;

import com.pardot.protobufs.ModelEventOuterClass;
import com.pardot.protobufs.Salesforce;
import io.confluent.ksql.GenericRow;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class KsqlProtobufDeserializerTest {

    private static final Logger logger = LoggerFactory.getLogger(KsqlProtobufDeserializerTest.class);

    private Schema orderSchema;

    @Before
    public void before() {

        orderSchema = SchemaBuilder.struct()
            .field("accountId".toUpperCase(), org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
            .field("owner".toUpperCase(), Schema.OPTIONAL_STRING_SCHEMA)
            .field("operation".toUpperCase(), org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            .field("apiVersion".toUpperCase(), org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    }

    @Test
    public void doTest() {
        // Create protobuf
        final Salesforce.WriteRequest protoBuf = Salesforce.WriteRequest.newBuilder()
            .setAccountId(1L)
            .setApiVersion("31.0")
            .setOperation(Salesforce.WriteRequest.Operation.DELETE)
            .setOwner("My Owner ID")
            .build();

        final byte[] bytes = protoBuf.toByteArray();

        // Define map
        final Map<String, Object> config = new HashMap<>();
        config.put(KsqlProtobufTopicSerDe.CONFIG_PROTOBUF_CLASS, Salesforce.WriteRequest.class.getName());
        //config.put(KsqlProtobufTopicSerDe.CONFIG_PROTOBUF_CLASS, ModelEventOuterClass.ModelEvent.class.getName());

        // Create
        final KsqlProtobufDeserializer ksqlProtobufDeserializer = new KsqlProtobufDeserializer(orderSchema);

        // Configure
        ksqlProtobufDeserializer.configure(config, false);

        // Deserialize
        final GenericRow genericRow = ksqlProtobufDeserializer.deserialize("", bytes);

        logger.info("Result: {}", genericRow);
    }

}