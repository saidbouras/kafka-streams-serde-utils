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
package com.sabo.commons;

import com.sabo.client.SchemaStorageClient;
import com.sabo.serdes.avro.config.KafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import java.util.HashMap;
import java.util.Map;

public class ConfigUtils {

    public static Map<String, Object> withSpecificAvroEnabled(Map<String, ?> config) {
        Map<String, Object> specificAvroEnabledConfig = (config == null) ? new HashMap<>() : new HashMap<>(config);
        specificAvroEnabledConfig.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return specificAvroEnabledConfig;
    }

    public static Map<String, Object> withSchemaStorageClient(Map<String, ?> config, Class<? extends SchemaStorageClient> tclass) {
        Map<String, Object> schemaStorageConfig = (config == null) ? new HashMap<>() : new HashMap<>(config);
        schemaStorageConfig.put(KafkaAvroSerDeConfig.SCHEMAS_STORAGE_LOCATION_CONFIG, tclass);
        return schemaStorageConfig;
    }
}