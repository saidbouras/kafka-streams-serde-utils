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
package com.sabo.serdes.avro.internal;

import com.sabo.client.SchemaStorageClient;
import com.sabo.schemas.primitives.PrimitivesSchemas;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.errors.SerializationException;

import java.nio.ByteBuffer;


public abstract class AbstractAvroSerDe extends PrimitivesSchemas {

    protected static final String SCHEMA_REGISTRY_SCHEMA_VERSION_PROP = "schema.registry.schema.version";
    protected static final byte MAGIC_BYTE = 0x0;
    protected static final int idSize = 4;
    protected boolean useSpecificAvroReader = false;

    protected SchemaStorageClient schemaClient;

    protected Schema getSchema(Object object) {
        if (object == null) {
            return getPrimitiveSchemas().get("Null");
        } else if (object instanceof Boolean) {
            return getPrimitiveSchemas().get("Boolean");
        } else if (object instanceof Integer) {
            return getPrimitiveSchemas().get("Integer");
        } else if (object instanceof Long) {
            return getPrimitiveSchemas().get("Long");
        } else if (object instanceof Float) {
            return getPrimitiveSchemas().get("Float");
        } else if (object instanceof Double) {
            return getPrimitiveSchemas().get("Double");
        } else if (object instanceof CharSequence) {
            return getPrimitiveSchemas().get("String");
        } else if (object instanceof byte[]) {
            return getPrimitiveSchemas().get("Bytes");
        } else if (object instanceof GenericContainer) {
            return ((GenericContainer) object).getSchema();
        } else {
            throw new IllegalArgumentException(
                "Unsupported Avro type. Supported types are null, Boolean, Integer, Long, "
                    + "Float, Double, String, byte[] and IndexedRecord");
        }
    }

    protected ByteBuffer getByteBuffer(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        if (buffer.get() != MAGIC_BYTE) {
            throw new SerializationException("Unknown magic byte!");
        } else {
            return buffer;
        }
    }

}