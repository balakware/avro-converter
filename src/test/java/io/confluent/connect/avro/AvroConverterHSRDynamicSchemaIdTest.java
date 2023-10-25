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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

//Run tests one by one as there is a Wire mock which tries to get configured for different use cases
public class AvroConverterHSRDynamicSchemaIdTest {
    private static final String TOPIC = "topic";
    private GenericRecord genericRecord;
    private Schema schema;
    private static Integer schemaIDcflt=0;
    private static utils ut= new utils();
    private WireMockServer wireMockServer;
    private static final Map<String, ?> SR_CONFIG = Collections.singletonMap(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost");

    private final SchemaRegistryClient schemaRegistry;
    private final AvroConverterHSRDynamicSchemaId converter;
    public AvroConverterHSRDynamicSchemaIdTest() {
        schemaRegistry = new MockSchemaRegistryClient();
        converter = new AvroConverterHSRDynamicSchemaId(schemaRegistry);
    }

    @Before
    public void setUp() throws RestClientException, IOException {

    wireMockServer = new WireMockServer(3080);
    wireMockServer.start();
    WireMock.configureFor("localhost", 3080);
    System.out.println("started");
    org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(empSchema());
        ParsedSchema scm2=new AvroSchema(new schemaParser().parse(empSchema()));
        schemaIDcflt= schemaRegistry.register(TOPIC+"-value", scm2,false);

    }
    public String empSchema(){
        return "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"Employee\",\n" +
                "  \"namespace\": \"org.hifly.kafka.demo.producer.serializer.avro\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"name\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"empid\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"gender\",\n" +
                "      \"type\": \"string\"\n" +
                "   },\n" +
                "   {\n" +
                "      \"name\": \"dept\",\n" +
                "      \"type\": \"string\"\n" +
                "  },\n" +
                "  {\n" +
                "        \"name\": \"location\",\n" +
                "        \"type\": \"string\"\n" +
                "    }\n" +
                "  ]\n" +
                "}\n";
    }
    @Test
    public void testCSRtoHSRDynamicSchemaIDAppendAutoRegisterTrue() throws Exception {
        Map<String,String> config=new HashMap<>();
        config.put("hortonworks.schema.registry.base.url","http://localhost:3080/api/v1/schemaregistry/schemas/");
        config.put("hortonworks.auto.register.schema","true");
        config.put("schema.registry.url","http://fake-url");
        config.put("hortonworks.protocol.version","3");
        config.put("hortonworks.schema.registry.schema.version.uri","/versions/");
        converter.configure(config,false);
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(empSchema());
        String jsonStr="{\"id\": \"1\"}";


        //Mock schema metadata creation
        WireMock.stubFor(WireMock.post(WireMock.urlEqualTo("/api/v1/schemaregistry/schemas/"))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("")));
        //Mock schema text creation

        WireMock.stubFor(WireMock.post(WireMock.urlEqualTo("/api/v1/schemaregistry/schemas/" + TOPIC + "/versions?branch=MASTER"))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("1")));
        //Mock get the schema version ID
        String URL="/api/v1/schemaregistry/schemas/" + TOPIC + "/versions/1";
        WireMock.stubFor(WireMock.get(WireMock.urlEqualTo(URL))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(jsonStr)));

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("name","john");
        avroRecord.put("empid","1001");
        avroRecord.put("gender","M");
        avroRecord.put("dept","F1");
        avroRecord.put("location","london");
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
    public void testCSRtoHSRDynamicSchemaIDAppendAutoRegisterFalse() throws Exception {
        Map<String,String> config=new HashMap<>();
        config.put("hortonworks.schema.registry.base.url","http://localhost:3080/api/v1/schemaregistry/schemas/");
        config.put("hortonworks.auto.register.schema","false");
        config.put("schema.registry.url","http://fake-url");
        config.put("hortonworks.protocol.version","3");
        config.put("hortonworks.schema.registry.schema.version.uri","/versions/");
        converter.configure(config,false);
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(empSchema());
        String jsonStr="{\"id\": \"1\"}";

        //Mock get the schema version ID
        String URL="/api/v1/schemaregistry/schemas/" + TOPIC + "/versions/1";
        WireMock.stubFor(WireMock.get(WireMock.urlEqualTo(URL))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(jsonStr)));

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("name","john");
        avroRecord.put("empid","1001");
        avroRecord.put("gender","M");
        avroRecord.put("dept","F1");
        avroRecord.put("location","london");
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
    public void testCSRtoHSRDynamicSchemaIDAppendAutoRegisterFalseButSchemaNotPresent() throws Exception {
        Map<String,String> config=new HashMap<>();
        config.put("hortonworks.schema.registry.base.url","http://localhost:3080/api/v1/schemaregistry/schemas/");
        config.put("hortonworks.auto.register.schema","false");
        config.put("schema.registry.url","http://fake-url");
        config.put("hortonworks.protocol.version","3");
        config.put("hortonworks.schema.registry.schema.version.uri","/versions/");
        converter.configure(config,false);
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(empSchema());
        String jsonStr="{\"id\": \"2\"}";

        //Mock get the schema version ID(this does not return the version expected by the code
        String URL="/api/v1/schemaregistry/schemas/" + TOPIC + "/versions/2";
        WireMock.stubFor(WireMock.get(WireMock.urlEqualTo(URL))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(jsonStr)));

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("name","john");
        avroRecord.put("empid","1001");
        avroRecord.put("gender","M");
        avroRecord.put("dept","F1");
        avroRecord.put("location","london");
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
        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter(avroSchema);
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