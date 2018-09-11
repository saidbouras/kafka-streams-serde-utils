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

import com.sabo.commons.ConfigUtils;
import com.sabo.schemas.storage.local.utils.LocalConfigUtils;
import com.sabo.serdes.avro.internal.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class LocalSpecificSerializer<T extends SpecificRecord> implements Serializer<T> {

    private final KafkaAvroSerializer inner;

    public LocalSpecificSerializer() {
        inner = new KafkaAvroSerializer();
    }

    @Override
    public void configure(final Map<String, ?> serializerConfig, final boolean isKey) {
        final Map<String, Object> withSpecificEnabled =
            ConfigUtils.withSpecificAvroEnabled(serializerConfig);
        inner.configure(LocalConfigUtils.withLocalSchemaStorage(withSpecificEnabled), isKey);
    }

    @Override
    public byte[] serialize(final String topic, final T record) {
        return inner.serialize(topic, record);
    }

    @Override
    public void close() {
        // do nothing
    }
}