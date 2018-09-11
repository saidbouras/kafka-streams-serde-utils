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
package com.sabo.schemas;

import org.apache.avro.Schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class PrimitivesSchemas {

    public static Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);
    public static Schema BYTES_SCHEMA = Schema.create(Schema.Type.BYTES);
    public static Schema INT_SCHEMA = Schema.create(Schema.Type.INT);
    public static Schema LONG_SCHEMA = Schema.create(Schema.Type.LONG);
    public static Schema FLOAT_SCHEMA = Schema.create(Schema.Type.FLOAT);
    public static Schema DOUBLE_SCHEMA = Schema.create(Schema.Type.DOUBLE);
    public static Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);
    public static Schema BOOLEAN_SCHEMA = Schema.create(Schema.Type.BOOLEAN);

    private static final Map<String, Schema> primitiveSchemas;
    static {
        primitiveSchemas = new HashMap<>();
        primitiveSchemas.put("Null", NULL_SCHEMA);
        primitiveSchemas.put("Boolean", BOOLEAN_SCHEMA);
        primitiveSchemas.put("Integer", INT_SCHEMA);
        primitiveSchemas.put("Long", LONG_SCHEMA);
        primitiveSchemas.put("Float", FLOAT_SCHEMA);
        primitiveSchemas.put("Double", DOUBLE_SCHEMA);
        primitiveSchemas.put("String", STRING_SCHEMA);
        primitiveSchemas.put("Bytes", BYTES_SCHEMA);
    }

    public static Map<String,Schema> getPrimitiveSchemas() {
        return Collections.unmodifiableMap(primitiveSchemas);
    }
}