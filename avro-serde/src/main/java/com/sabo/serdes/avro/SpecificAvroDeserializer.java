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
package com.sabo.serdes.avro;

import com.sabo.client.SchemaStorageClient;
import com.sabo.commons.ConfigUtils;
import com.sabo.serdes.avro.internal.KafkaAvroDeserializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class SpecificAvroDeserializer<T extends SpecificRecord> implements Deserializer<T> {

    private final KafkaAvroDeserializer inner;

    public SpecificAvroDeserializer() {
        inner = new KafkaAvroDeserializer();
    }

    /**
     * For testing purposes only.
     */
    SpecificAvroDeserializer(final SchemaStorageClient client) {
        inner = new KafkaAvroDeserializer(client);
    }

    @Override
    public void configure(final Map<String, ?> deserializerConfig, final boolean isKey) {
        inner.configure(ConfigUtils.withSpecificAvroEnabled(deserializerConfig), isKey);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(final String topic, final byte[] bytes) {
        return (T) inner.deserialize(topic, bytes);
    }

    @Override
    public void close() {
        inner.close();
    }

}