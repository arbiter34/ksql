/**
 * Copyright 2017 Confluent Inc.
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
 **/

package io.confluent.ksql.serde.protobuf;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.json.KsqlJsonSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class KsqlProtobufSerializer implements Serializer<GenericRow> {

  private static final Logger logger = LoggerFactory.getLogger(KsqlProtobufSerializer.class);
  private final KsqlJsonSerializer ksqlJsonSerializer;

  private Class protobufType = null;
  private Method newBuilderMethod = null;
  private JsonFormat.Parser jsonFormatParser = null;

  /**
   * Default constructor needed by Kafka
   */
  public KsqlProtobufSerializer(final Schema schema) {
    ksqlJsonSerializer = new KsqlJsonSerializer(schema);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(final Map<String, ?> props, final boolean isKey) {
    logger.info("Got properties: {}", props);

    // Get class from config
    final String classStr = (String) props.get(KsqlProtobufTopicSerDe.CONFIG_PROTOBUF_CLASS);
    try {
      protobufType = Class.forName(classStr);
      //protobufType = getClass().getClassLoader().loadClass(classStr);
      newBuilderMethod = protobufType.getMethod("newBuilder");
      jsonFormatParser = JsonFormat.parser();
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      // TODO handle.
      logger.error("Caught exception: {}", e.getMessage(), e);
      throw new RuntimeException(e.getMessage(), e);
    }

    ksqlJsonSerializer.configure(props, isKey);
  }

  @Override
  public byte[] serialize(final String topic, final GenericRow data) {
    // Convert from GenericRow into JSON (bytes)
    final byte[] jsonBytes = ksqlJsonSerializer.serialize(topic, data);
    if (jsonBytes == null) {
      return null;
    }

    try {
      // Convert back to string
      final String jsonStr = new String(jsonBytes, StandardCharsets.UTF_8);

      // Create new builder
      final Message.Builder builder = (Message.Builder) newBuilderMethod.invoke(protobufType);

      // Load JSON into builder
      jsonFormatParser.merge(jsonStr, builder);

      // serialize protobuf to byte array
      return builder.build().toByteArray();
    } catch (
        IllegalAccessException
        | InvocationTargetException
        | InvalidProtocolBufferException e
    ) {
      // TODO handle.
      e.printStackTrace();
    }

    return null;
  }


  @Override
  public void close() {
  }

}