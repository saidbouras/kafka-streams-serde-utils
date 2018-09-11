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
import com.sabo.serdes.avro.config.KafkaAvroSerDeConfig;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaAvroSerializer extends AbstractAvroSerializer implements Serializer<Object> {

    private boolean isKey;

    /**
     * Constructor used by Kafka producer.
     */
    public KafkaAvroSerializer() {
    }

    public KafkaAvroSerializer(SchemaStorageClient client) {
        schemaClient = client;
    }

    public KafkaAvroSerializer(SchemaStorageClient client, Map<String, ?> props) {
        schemaClient = client;
        configure(new KafkaAvroSerDeConfig(props));
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        configure(new KafkaAvroSerDeConfig(configs));
    }

    @Override
    public byte[] serialize(String topic, Object record) {
        return serializeImpl(schemaClient.getSubjectName(topic, isKey, record), record);
    }

    @Override
    public void close() {

    }
}
