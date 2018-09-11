/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sabo.avro.tuples;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.ClassUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.sabo.schemas.PrimitivesSchemas.*;

public class SpecificTuplesData extends SpecificData {

    private static final SpecificTuplesData INSTANCE = new SpecificTuplesData();

    private static final int MIN_TUPLE_SIZE = 2;
    private static final int MAX_TUPLE_SIZE = 11;

    private static final List<String> tuplesFields =
        IntStream.range(MIN_TUPLE_SIZE - 1, MAX_TUPLE_SIZE +1).mapToObj(i -> "v" + i).collect(Collectors.toList());

    public static SpecificTuplesData get() {
        return INSTANCE;
    }

    public boolean isAssignableFrom(Class<? extends SpecificRecord> readerClass) {
        return Tuple.class.isAssignableFrom(readerClass);
    }

    public static String tuplesNamespace() {
        return Tuple.class.getPackage().getName();
    }

    public static boolean isTuples(Schema schema) {
        Map<String, Schema> primitiveSchemas = getPrimitiveSchemas();
        final boolean predicate =
            !primitiveSchemas.containsValue(schema)
                && schema.getName().startsWith(Tuple.class.getSimpleName())
                && tuplesNamespace().equals(schema.getNamespace());

        if (predicate) {
            final List<String> fieldsNames = schema.getFields().stream()
                .map(Schema.Field::name)
                .collect(Collectors.toList());

            return tuplesFields.containsAll(fieldsNames);
        }
        return false;
    }

    /**
     * This method will be private when we will use caching with
     * @see #getTupleSchema
     */
    public Schema makeTupleSchemas(String name, List<Schema> schemas) {
        if (schemas.size() < MIN_TUPLE_SIZE || schemas.size() > MAX_TUPLE_SIZE) {
            throw new IllegalArgumentException("Tuple" + schemas.size() + " doesn't exist !");
        }
        Schema tuple = Schema.createRecord(name, null, tuplesNamespace(), false);

        List<Schema.Field> fields = IntStream.range(0, schemas.size() - 1)
            .mapToObj(i -> new Schema.Field(tuplesFields.get(i), schemas.get(i), "", (Object) null))
            .collect(Collectors.toList());

        int lastIndex = schemas.size() - 1;
        fields.add(new Schema.Field(tuplesFields.get(lastIndex), schemas.get(lastIndex), "", (Object) null, Schema.Field.Order.IGNORE));

        tuple.setFields(fields);
        return tuple;
    }

    private static final Map<Schema, Map<Schema, Schema>> SCHEMA_CACHE = new WeakHashMap<>();

    /**
     * Get a tuple schema.
     */
    public Schema getTupleSchema(String name, Schema s1, Schema s2) {
        Map<Schema, Schema> valueSchemas;
        synchronized (SCHEMA_CACHE) {
            valueSchemas = SCHEMA_CACHE.computeIfAbsent(s1, k -> new WeakHashMap<>());
            Schema result;
            result = valueSchemas.get(s2);
            if (result == null) {
                result = makeTupleSchemas(name, Arrays.asList(s1, s2));
                valueSchemas.put(s2, result);
            }
            return result;
        }
    }

    public Schema getSchema(Object obj) {
        Schema result;
        if (obj instanceof GenericContainer) {
            result = ((GenericContainer) obj).getSchema();
        } else if (obj instanceof CharSequence) {
            result = STRING_SCHEMA;
        } else if (obj instanceof ByteBuffer) {
            result = BYTES_SCHEMA;
        } else if (obj instanceof Integer) {
            result = INT_SCHEMA;
        } else if (obj instanceof Long) {
            result = LONG_SCHEMA;
        } else if (obj instanceof Float) {
            result = FLOAT_SCHEMA;
        } else if (obj instanceof Double) {
            result = DOUBLE_SCHEMA;
        } else if (obj instanceof Void) {
            result = NULL_SCHEMA;
        } else {
            result = inferSchema(obj);
        }
        return result;
    }

    protected Schema inferSchema(Object o) {
        try {
            return ReflectData.get().getSchema(o.getClass());
        } catch (AvroRuntimeException e) {
            throw new AvroRuntimeException
                ("Cannot infer schema for : " + o.getClass()
                    + ".  Must create Tuple2 with explicit _1 and _2 schemas.", e);
        }
    }


    private Map<String, Class> classCache = new ConcurrentHashMap<String, Class>();

    private static final Class NO_CLASS = new Object() {
    }.getClass();
    private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);

    /**
     * Return the class that implements a schema, or null if none exists.
     */
    public Class getClass(Schema schema) {
        switch (schema.getType()) {
            case FIXED:
            case RECORD:
            case ENUM:
                String name = schema.getFullName();
                if (name == null) return null;
                Class c = classCache.get(name);
                if (c == null) {
                    try {
                        c = ClassUtils.forName(getClassLoader(), getClassName(schema));
                    } catch (ClassNotFoundException e) {
                        c = NO_CLASS;
                    }
                    classCache.put(name, c);
                }
                return c == NO_CLASS ? null : c;
            case ARRAY:
                return List.class;
            case MAP:
                return Map.class;
            case UNION:
                List<Schema> types = schema.getTypes();     // elide unions with null
                if ((types.size() == 2) && types.contains(NULL_SCHEMA))
                    return getWrapper(types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0));
                return Object.class;
            case STRING:
                if (STRING_TYPE_STRING.equals(schema.getProp(STRING_PROP)))
                    return String.class;
                return CharSequence.class;
            case BYTES:
                return ByteBuffer.class;
            case INT:
                return Integer.TYPE;
            case LONG:
                return Long.TYPE;
            case FLOAT:
                return Float.TYPE;
            case DOUBLE:
                return Double.TYPE;
            case BOOLEAN:
                return Boolean.TYPE;
            case NULL:
                return Void.TYPE;
            default:
                throw new AvroRuntimeException("Unknown type: " + schema);
        }
    }

    private Class getWrapper(Schema schema) {
        switch (schema.getType()) {
            case INT:
                return Integer.class;
            case LONG:
                return Long.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case BOOLEAN:
                return Boolean.class;
        }
        return getClass(schema);
    }


    /**
     * Returns the Java class name indicated by a schema's name and tuplesNamespace.
     */
    public static String getClassName(Schema schema) {
        String namespace = schema.getNamespace();
        String name = schema.getName();
        if (isTuples(schema)) {
            name = getTupleClassName(schema);
        }
        if (namespace == null || "".equals(namespace))
            return name;
        String dot = namespace.endsWith("$") ? "" : ".";
        return namespace + dot + name;
    }

    private static String getTupleClassName(Schema schema) {
        final List<Schema.Field> fields = schema.getFields();
        if (fields.size() < MIN_TUPLE_SIZE || fields.size() > MAX_TUPLE_SIZE) {
            throw new IllegalArgumentException("Tuple" + fields.size() + " doesn't exist !");
        }
        return Tuple.class.getSimpleName() + fields.size();
    }
}
