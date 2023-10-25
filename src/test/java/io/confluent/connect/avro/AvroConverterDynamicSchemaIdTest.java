package io.confluent.connect.avro;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.avro.AvroTypeException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.apache.avro.specific.SpecificDatumWriter;


import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;



import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

//Run tests one by one as there is a Wire mock which tries to get configured for different use cases
public class AvroConverterDynamicSchemaIdTest {
    private static final String TOPIC = "topic-3";
    private GenericRecord genericRecord;
    private Schema schema;
    private static utils ut= new utils();
    private static Integer schemaIDcflt=0;
    private WireMockServer wireMockServer;
    private static final Map<String, ?> SR_CONFIG = Collections.singletonMap(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost");

    private final SchemaRegistryClient schemaRegistry;
    private final AvroConverterDynamicSchemaId converter;
    public AvroConverterDynamicSchemaIdTest() {
        schemaRegistry = new MockSchemaRegistryClient();
        converter = new AvroConverterDynamicSchemaId(schemaRegistry);
    }

    @Before
    public void setUp() throws RestClientException, IOException {
    wireMockServer = new WireMockServer(3080);
    wireMockServer.start();
    WireMock.configureFor("localhost", 3080);
    System.out.println("started");


    }
    public String carSchema(){
        return "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"Car\",\n" +
                "  \"namespace\": \"org.hifly.kafka.demo.producer.serializer.avro\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"model\",\n" +
                "      \"type\": \"string\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";
    }


    @Test
    public void testHSRtoCSRDynamicSchemaIDAppend() throws Exception {
        Map<String,String> config=new HashMap<>();
        config.put("hortonworks.schema.registry.base.url","http://localhost:3080/api/v1/schemaregistry/schemas/");
        config.put("confluent.auto.register.schema","true");
        config.put("schema.registry.url","http://fake-url");
        converter.configure(config,false);
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(carSchema());
        String jsonStr="{\"schemaText\": \"{\\n  \\\"type\\\": \\\"record\\\",\\n  \\\"name\\\": \\\"Car\\\",\\n  \\\"namespace\\\": \\\"org.hifly.kafka.demo.producer.serializer.avro\\\",\\n  \\\"fields\\\": [\\n    {\\n      \\\"name\\\": \\\"model\\\",\\n      \\\"type\\\": \\\"string\\\"\\n    }\\n  ]\\n}\\n\",\"version\": \"1\"}";
        WireMock.stubFor(WireMock.get(WireMock.urlEqualTo("/api/v1/schemaregistry/schemas/versionsById/2"))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(jsonStr)));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("model","audi");
        out.write(3) ;
        out.write(ByteBuffer.allocate(4).putInt(2).array());
        byte[] avroByteArray = avroRecordToByteArray(avroRecord, schema);
        out.write(avroByteArray);
        byte[] converted = converter.fromConnectData(TOPIC,null,null,out.toByteArray());
        byte [] scm=Arrays.copyOfRange(converted, 1, 5);
        int schemaVersionId = new BigInteger(scm).intValue() ;
        byte [] data= Arrays.copyOfRange(converted,5,converted.length);
        System.out.println(schemaVersionId);
        //This test proves that we are appending the right schema ID to the data
        assertEquals(1, schemaVersionId);
        //This test proves that the data is not corrupted while the transformation is happening
        assertEquals(true,validateData(data,schema));
    }
    @Test
    public void testHSRtoCSRDynamicSchemaIDAppendAutoRegisterFalse() throws Exception {
        //The schema should be registered into CSR as well beforehand
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(carSchema());
        ParsedSchema scm2=new AvroSchema(new schemaParser().parse(carSchema()));
        schemaIDcflt= schemaRegistry.register(TOPIC+"-value", scm2,false);
        Map<String,String> config=new HashMap<>();
        config.put("hortonworks.schema.registry.base.url","http://localhost:3080/api/v1/schemaregistry/schemas/");
        config.put("confluent.auto.register.schema","false");
        config.put("schema.registry.url","http://fake-url");
        converter.configure(config,false);
        String jsonStr="{\"schemaText\": \"{\\n  \\\"type\\\": \\\"record\\\",\\n  \\\"name\\\": \\\"Car\\\",\\n  \\\"namespace\\\": \\\"org.hifly.kafka.demo.producer.serializer.avro\\\",\\n  \\\"fields\\\": [\\n    {\\n      \\\"name\\\": \\\"model\\\",\\n      \\\"type\\\": \\\"string\\\"\\n    }\\n  ]\\n}\\n\",\"version\": \"1\"}";;

        //Mock get the schema version ID
        String URL="/api/v1/schemaregistry/schemas/versionsById/1";
        WireMock.stubFor(WireMock.get(WireMock.urlEqualTo(URL))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(jsonStr)));

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("model","audi");
        out.write(1) ;
        out.write(ByteBuffer.allocate(4).putInt(1).array());
        byte[] avroByteArray = avroRecordToByteArray(avroRecord, schema);
        out.write(avroByteArray);
        byte[] converted = converter.fromConnectData(TOPIC,null,null,out.toByteArray());
        byte [] scm=Arrays.copyOfRange(converted, 1, 5);
        int schemaVersionId = new BigInteger(scm).intValue() ;
        byte [] data= Arrays.copyOfRange(converted,5,converted.length);
        System.out.println(schemaVersionId);
        //This test proves that we are appending the right schema ID to the data
        assertEquals(1, schemaVersionId);
        //This test proves that the data is not corrupted while the transformation is happening
        assertEquals(true,validateData(data,schema));
    }
    @Test
    public void testHSRtoCSRDynamicSchemaIDAppendAutoRegisterFalseButSchemaNotPresent() throws Exception {
        //We will not register the schema
        Map<String,String> config=new HashMap<>();
        config.put("hortonworks.schema.registry.base.url","http://localhost:3080/api/v1/schemaregistry/schemas/");
        config.put("confluent.auto.register.schema","false");
        config.put("schema.registry.url","http://fake-url");
        converter.configure(config,false);
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(carSchema());
        String jsonStr="{\"schemaText\": \"{\\n  \\\"type\\\": \\\"record\\\",\\n  \\\"name\\\": \\\"Car\\\",\\n  \\\"namespace\\\": \\\"org.hifly.kafka.demo.producer.serializer.avro\\\",\\n  \\\"fields\\\": [\\n    {\\n      \\\"name\\\": \\\"model\\\",\\n      \\\"type\\\": \\\"string\\\"\\n    }\\n  ]\\n}\\n\",\"version\": \"1\"}";;
        String URL="/api/v1/schemaregistry/schemas/versionsById/1";
        WireMock.stubFor(WireMock.get(WireMock.urlEqualTo(URL))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(jsonStr)));

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("model","audi");
        out.write(1) ;
        out.write(ByteBuffer.allocate(4).putInt(1).array());
        byte[] avroByteArray = avroRecordToByteArray(avroRecord, schema);
        out.write(avroByteArray);
        try{
            byte[] converted = converter.fromConnectData(TOPIC,null,null,out.toByteArray());
        }

        catch(Exception e){

            Throwable cause = e;
            while(cause.getCause() != null && cause.getCause() != cause) {
                cause = cause.getCause();
            }
            //The test should throw an exception message stating the fact the schema should either be present
            //in the registry or auto register should be true
            assertTrue(cause.toString().contains("Auto register should " +
                    "be configured to register schemas automatically or schemas must be present in registry"));
        }
    }

    public static byte[] avroRecordToByteArray(GenericRecord avroRecord,  org.apache.avro.Schema avroSchema) throws Exception {
        DatumWriter<GenericRecord> datumWriter = new org.apache.avro.specific.SpecificDatumWriter(avroSchema);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

        datumWriter.write(avroRecord, encoder);
        encoder.flush();

        return outputStream.toByteArray();
    }
    public static Boolean validateData(byte[] byteArray, org.apache.avro.Schema avroSchema) throws Exception {
        GenericDatumReader<GenericRecord> datumReader =  new GenericDatumReader(avroSchema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(byteArray, null);
        try{
            datumReader.read(null, decoder);
            return true;
        }
        catch (AvroTypeException e){
            return false;
        }
    }

}