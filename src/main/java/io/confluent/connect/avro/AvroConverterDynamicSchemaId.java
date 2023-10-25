/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.avro;

import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.*;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


//import com.google.common.collect.MapMaker;

/**
 * Implementation of Converter that uses Avro schemas and objects.
 */
public class AvroConverterDynamicSchemaId implements Converter {

  private SchemaRegistryClient schemaRegistry;
  private static HttpURLConnection conn =null;
  private static final EncoderFactory encoderFactory = EncoderFactory.get();
  private static final Map<Integer,Integer> schemaMap=new HashMap<>();
  private  static final utils util=new utils();
  private Serializer serializer;
  private Deserializer deserializer;

  private boolean isKey;

  public AvroData avroData;
  private static String hortonSRbaseURL=null;
 // private static String schemaTextUri=null;
  private static Boolean autoregister=false;
//  private static final Map<org.apache.avro.Schema, DatumWriter<Object>> datumWriterCache =
//          new MapMaker().weakKeys().makeMap();
  //protected AvroMapper objectMapper = new ObjectMapper.DefaultTypeResolverBuilder();

  public AvroConverterDynamicSchemaId() {
  }

  // Public only for testing
  public AvroConverterDynamicSchemaId(SchemaRegistryClient client) {
    schemaRegistry = client;
  }


  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    this.hortonSRbaseURL= ((String)configs.get("hortonworks.schema.registry.base.url"));
    //this.schemaTextUri= ((String)configs.get("hortonworks.schema.registry.schemaText.uri"));
    this.autoregister= Boolean.valueOf((String)configs.get("confluent.auto.register.schema"));

    AvroConverterConfig avroConverterConfig = new AvroConverterConfig(configs);

    if (schemaRegistry == null) {
      schemaRegistry = new CachedSchemaRegistryClient(
          avroConverterConfig.getSchemaRegistryUrls(),
          avroConverterConfig.getMaxSchemasPerSubject(),
          Collections.singletonList(new AvroSchemaProvider()),
          configs,
          avroConverterConfig.requestHeaders()
      );
    }

    serializer = new Serializer(configs, schemaRegistry);
    deserializer = new Deserializer(configs, schemaRegistry);
    avroData = new AvroData(new AvroDataConfig(configs));
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    try {
      //Object valueAvro = avroData.fromConnectData(schema,avroSchema,value);

      return serializer.serialize(
          topic,
          false,
          value);
    } catch (SerializationException e) {
      throw new DataException(
          String.format("Failed to serialize Avro data from topic %s :", topic),
          e
      );
    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format("Failed to access Avro data from topic %s : %s", topic, e.getMessage())
      );
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    try {
      GenericContainerWithVersion containerWithVersion =
          deserializer.deserialize(topic, isKey, value);
      if (containerWithVersion == null) {
        return SchemaAndValue.NULL;
      }

      GenericContainer deserialized = containerWithVersion.container();
      Integer version = containerWithVersion.version();
      if (deserialized instanceof IndexedRecord) {
        return avroData.toConnectData(deserialized.getSchema(), deserialized, version);
      } else if (deserialized instanceof NonRecordContainer) {
        return avroData.toConnectData(
            deserialized.getSchema(), ((NonRecordContainer) deserialized).getValue(), version);
      }
      throw new DataException(
          String.format("Unsupported type returned during deserialization of topic %s ", topic)
      );
    } catch (SerializationException e) {
      throw new DataException(
          String.format("Failed to deserialize data for topic %s to Avro: ", topic),
          e
      );
    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format("Failed to access Avro data from topic %s : %s", topic, e.getMessage())
      );
    }
  }


  static class Serializer extends AbstractKafkaAvroSerializer {

    public Serializer(SchemaRegistryClient client, boolean autoRegisterSchema) {
      schemaRegistry = client;
      this.autoRegisterSchema = autoRegisterSchema;
    }

    public Serializer(Map<String, ?> configs, SchemaRegistryClient client) {

      this(client, false);
      configure(new KafkaAvroSerializerConfig(configs));
    }

    public byte[] serialize(
        String topic, boolean isKey, Object value) throws IOException {
      if (value == null) {
        return null;
      }
      return serializeSimplifiedImpl(topic,value);
    }
    public byte[] serializeSimplifiedImpl(
            String topic, Object object)
            throws SerializationException, InvalidConfigurationException, IOException {
//      System.out.println("custom avro serializer was called");
    // System.out.println("test**");
      byte [] temp= (byte[]) object;
    //  System.out.println("proto--" + Array.get(temp,0).toString());
      byte [] scm=Arrays.copyOfRange(temp, 1, 5);
      int schemaVersionId = new BigInteger(scm).intValue() ;
      //System.out.println("schemdiver--" + (schemaVersionId));
      String subject=topic+"-value";
      String schemaText=null;
      if (schemaRegistry == null) {
        StringBuilder userFriendlyMsgBuilder = new StringBuilder();
        userFriendlyMsgBuilder.append("You must configure() before serialize()");
        userFriendlyMsgBuilder.append(" or use serializer constructor with SchemaRegistryClient");
        throw new InvalidConfigurationException(userFriendlyMsgBuilder.toString());
      }
      // null needs to treated specially since the client most likely just wants to send
      // an individual null value instead of making the subject a null type. Also, null in
      // Kafka has a special meaning for deletion in a topic with the compact retention policy.
      // Therefore, we will bypass schema registration and return a null value in Kafka, instead
      // of an Avro encoded null.
      if (object == null) {
        return null;
      }
      String restClientErrorMsg = "";
      try {
        //System.out.println("in" + subject);
        int id = 0;
        id=util.getCsrSchemaId
                (subject,schemaMap,schemaVersionId,
                        hortonSRbaseURL,autoregister,schemaRegistry);
        schemaMap.put(schemaVersionId,id);
        //System.out.println("the Id of the schema is :*****" +id);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(MAGIC_BYTE) ;
        out.write(ByteBuffer.allocate(idSize).putInt(id).array());

        //System.out.println("data--" + new String(temp));
        //System.out.println("Source byte length" + (temp.length));

        byte[] slice = Arrays.copyOfRange(temp, 5, (temp.length));
        out.write(slice);
        byte[] bytes = out.toByteArray();
        //System.out.println("Destination byte length" + (bytes.length));
        //System.out.println("done");
        out.close();
        return bytes;
      }
        catch (IOException | RuntimeException e) {
        // avro serialization can throw AvroRuntimeException, NullPointerException,
        // ClassCastException, etc
        throw new SerializationException("Error serializing Avro message", e);} catch (RestClientException e) {
        throw new RuntimeException(e);
      }
    }


  }
  private static class Deserializer extends AbstractKafkaAvroDeserializer {

    public Deserializer(SchemaRegistryClient client) {
      schemaRegistry = client;
    }

    public Deserializer(Map<String, ?> configs, SchemaRegistryClient client) {
      this(client);
      configure(new KafkaAvroDeserializerConfig(configs));
    }

    public GenericContainerWithVersion deserialize(String topic, boolean isKey, byte[] payload) {
      return deserializeWithSchemaAndVersion(topic, isKey, payload);
    }
  }
}
