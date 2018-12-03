package io.confluent.ksql.parser.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.confluent.ksql.ddl.DdlConfig;
import io.confluent.ksql.parser.tree.Array;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.Struct;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.util.Pair;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class KsqlProtobufSchemaBuilder {

    private final String classStr;
    public KsqlProtobufSchemaBuilder(final Map<String, Expression> props) {
        // Get class from config
        classStr = ((StringLiteral)props.get(DdlConfig.PROTOBUF_CLASS_PROPERTY)).getValue();
    }

    @SuppressWarnings("unchecked")
    public List<TableElement> buildSchema() {
        try {
            final Class protobufType = Class.forName(classStr);
            final Method newBuilderMethod = protobufType.getMethod("newBuilder");
            final Message.Builder builder = (Message.Builder) newBuilderMethod.invoke(protobufType);
            return buildSchema(builder.getDescriptorForType())
                    .stream()
                    .map(pair -> new TableElement(pair.left, pair.right))
                    .collect(Collectors.toList());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private List<Pair<String, Type>> buildSchema(final Descriptors.Descriptor descriptor) {
        return descriptor
                .getFields()
                .stream()
                .map(fieldDescriptor -> new Pair<>(fieldDescriptor.getName().toUpperCase(), getType(fieldDescriptor)))
                .filter(pair -> Objects.nonNull(pair.right))
                .collect(Collectors.toList());
    }

    private Type getType(final Descriptors.FieldDescriptor fieldDescriptor) {
        if (fieldDescriptor.isRepeated()) {
            return new Array(_getType(fieldDescriptor));
        } else if (fieldDescriptor.isMapField()) {
            return new io.confluent.ksql.parser.tree.Map(_getType(fieldDescriptor.getMessageType().getFields().get(1)));
        } else {
            return _getType(fieldDescriptor);
        }
    }

    private Type _getType(final Descriptors.FieldDescriptor fieldDescriptor) {
        switch (fieldDescriptor.getType()) {
            case DOUBLE:
            case FLOAT:
                return new PrimitiveType(Type.KsqlType.DOUBLE);
            case FIXED32:
            case INT32:
            case UINT32:
            case SFIXED32:
            case SINT32:
                return new PrimitiveType(Type.KsqlType.INTEGER);
            case FIXED64:
            case INT64:
            case UINT64:
            case SFIXED64:
            case SINT64:
                return new PrimitiveType(Type.KsqlType.BIGINT);
            case BOOL:
                return new PrimitiveType(Type.KsqlType.BOOLEAN);
            case STRING:
            case ENUM:
                return new PrimitiveType(Type.KsqlType.STRING);
            case GROUP:
            case MESSAGE:
                return new Struct(buildSchema(fieldDescriptor.getMessageType()));
            case BYTES:
                return null;
            default:
                throw new IllegalStateException("Unable to parse field of type " + fieldDescriptor.getType());
        }
    }
}
