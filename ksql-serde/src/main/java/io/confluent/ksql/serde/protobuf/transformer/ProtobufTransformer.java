package io.confluent.ksql.serde.protobuf.transformer;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Internal;
import com.google.protobuf.MapEntry;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.confluent.ksql.GenericRow;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Given a Schema definition, this class will transform a given ProtocolBuffer into the corresponding
 * GenericRow representation.
 *
 * WIP / Extremely rough.
 */
public class ProtobufTransformer {
  private final Schema schema;

  /**
   * Constructor.
   *
   * @param schema Schema definition.
   */
  public ProtobufTransformer(final Schema schema) {
    this.schema = schema;
  }

  /**
   * Converts a ProtocolBuffer message into its corresponding GenericRow representation.
   *
   * @param protobuf ProtocolBuffer message to convert.
   * @return GenericRow representing the ProtocolBuffer as defined by Schema.
   */
  public GenericRow convert(final MessageOrBuilder protobuf) {
    final List<Object> values = new ArrayList<>();

    // Loop over each field
    for (final Field field : schema.fields()) {
      values.add(convertField(field, protobuf));
    }

    return new GenericRow(values);
  }

  @SuppressWarnings("unchecked")
  private Object convertField(final Field field, final MessageOrBuilder protobuf) {
    // Get name of field
    final String name = field.name();

    // Find the field.
    // TODO cache this?
    for (final Descriptors.FieldDescriptor fieldDescriptor : protobuf.getDescriptorForType().getFields()) {
      if (!fieldDescriptor.getName().equalsIgnoreCase(name)) {
        continue;
      }

      // Get the field's value.
      final Object value = protobuf.getField(fieldDescriptor);

      // Handle null case.
      if (value == null) {
        return null;
      }
      return convertValue(field.schema(), fieldDescriptor, value);
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public Object convertValue(final Schema schema, final Descriptors.FieldDescriptor fieldDescriptor, final Object value) {
    // If this field is an ENUM...
    if (schema.type() != Schema.Type.ARRAY &&
        schema.type() != Schema.Type.MAP &&
        fieldDescriptor.getType().name().equals("ENUM")) {
      // Determine if we should return the name (string) or ordinal value (number)
      switch (schema.type()) {
        case INT8:
        case INT16:
        case INT32:
        case INT64:
          return ((Internal.EnumLite) value).getNumber();

        case STRING:
          return ((Descriptors.GenericDescriptor) value).getName();

        default:
          // TODO - ERROR!
          throw new RuntimeException("Type mismatch?");
      }
    }

    // Based on the type of the field..
    switch (schema.type()) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT32:
      case FLOAT64:
      case BOOLEAN:
        return value;
      case STRING:
        if (value instanceof Timestamp) {
          return Timestamps.toString((Timestamp) value);
        }
        return value;
      case ARRAY:
        final List values = (List) value;
        final List newValues = new ArrayList<>();

        for (final Object o : values) {
          newValues.add(convertValue(schema.valueSchema(), fieldDescriptor, o));
        }
        return newValues;

      case BYTES:
        return ((ByteString) value).toByteArray();

      case MAP:
        final Map mapValue = new HashMap<>();
        for (final MapEntry<Object, Object> mapEntry : (Collection<MapEntry>) value) {
          mapValue.put(convertValue(schema.keySchema(), fieldDescriptor, mapEntry.getKey()), convertValue(schema.valueSchema(), fieldDescriptor, mapEntry.getValue()));
        }
        return mapValue;

      case STRUCT:
        final Struct struct = new Struct(schema);
        for (final Field subField : schema.fields()) {
          struct.put(subField.name(), convertField(subField, (MessageOrBuilder) value));
        }
        return struct;
    }
    return null;
  }

  public Message convert(final GenericRow genericRow, final Message.Builder builder) {
    // Loop over each field
    final Iterator<Object> fieldValueIterator = genericRow.getColumns().iterator();
    for (final Field field : schema.fields()) {
      for (final Descriptors.FieldDescriptor fieldDescriptor : builder.getDescriptorForType().getFields()) {
        if (!fieldDescriptor.getName().equalsIgnoreCase(field.name())) {
          continue;
        }
        final Object value = fieldValueIterator.next();
        final Object protoValue = getValue(builder, fieldDescriptor, field.schema(), value);
        builder.setField(fieldDescriptor, protoValue);
      }
    }

    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private Object getValue(final Message.Builder builder, final Descriptors.FieldDescriptor fieldDescriptor, final Schema schema, final Object value) {
    // If this field is an ENUM...
    if (schema.type() != Schema.Type.ARRAY &&
        schema.type() != Schema.Type.MAP &&
        fieldDescriptor.getType().name().equals("ENUM")) {
      // Determine if we should return the name (string) or ordinal value (number)
      switch (schema.type()) {
        case INT8:
        case INT16:
        case INT32:
        case INT64:
          return fieldDescriptor.getEnumType().findValueByNumber((Integer) value);
        case STRING:
          return fieldDescriptor.getEnumType().findValueByName((String) value);
        default:
          // TODO - ERROR!
          throw new RuntimeException("Type mismatch?");
      }
    }
    // Based on the type of the field..
    switch (schema.type()) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT32:
      case FLOAT64:
      case BOOLEAN:
      case STRING:
        return value;

      case ARRAY:
        final List<Object> values = (List) value;
        final List<Object> convertedValues = new ArrayList<>();
        for (Object o : values) {
          convertedValues.add(getValue(builder, fieldDescriptor, schema.valueSchema(), o));
        }
        return convertedValues;

      case BYTES:
        return ByteString.copyFrom((byte[]) value);

      case MAP:
        final Descriptors.FieldDescriptor keyField = fieldDescriptor.getMessageType().getFields().get(0);
        final Descriptors.FieldDescriptor valueField = fieldDescriptor.getMessageType().getFields().get(1);
        final Map<Object, Object> keyValues = new HashMap<>();
        for (final Map.Entry entry : ((Map<Object, Object>) value).entrySet()) {
          keyValues.put(getValue(builder, keyField, schema.keySchema(), entry.getKey()),
                  getValue(builder, valueField, schema.valueSchema(), entry.getValue()));
        }
        return keyValues;

      case STRUCT:
        final Message.Builder subBuilder = builder.newBuilderForField(fieldDescriptor);
        final Struct struct = (Struct) value;
        for (final Field subField : struct.schema().fields()) {
          for (final Descriptors.FieldDescriptor subFieldDescriptor : subBuilder.getDescriptorForType().getFields()) {
            if (!subFieldDescriptor.getName().equalsIgnoreCase(subField.name())) {
              continue;
            }
            final Object subValue = struct.get(subField);
            subBuilder.setField(subFieldDescriptor, getValue(subBuilder, subFieldDescriptor, subField.schema(), subValue));
          }
        }
        return subBuilder.build();

      default:
        throw new IllegalStateException(String.format("Unknown type specified in Schema: %s", schema.type()));
    }
  }
}
