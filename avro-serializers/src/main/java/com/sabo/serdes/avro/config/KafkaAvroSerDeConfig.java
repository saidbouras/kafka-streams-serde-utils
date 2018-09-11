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
package com.sabo.serdes.avro.config;

import com.sabo.client.SchemaStorageClient;
import com.sabo.client.WrappedSchemaRegistryClient;
import io.confluent.common.config.ConfigDef;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

import java.util.Map;

public class KafkaAvroSerDeConfig extends AbstractKafkaAvroSerDeConfig {

    public static final String SCHEMAS_STORAGE_LOCATION_CONFIG = "schemas.storage.location";
    public static final Class<? extends SchemaStorageClient>
        SCHEMAS_STORAGE_LOCATION_DEFAULT = WrappedSchemaRegistryClient.class;

    public static final String SCHEMAS_STORAGE_LOCATION_DOC =
        "Specify a client class that contact schema storage location." +
            "By default the class is a schema registry wrapper, to provide other type of schema storage location " +
            "we have to implement the " + SchemaStorageClient.class + "'.";

    final KafkaAvroDeserializerConfig deserConfig;
    final KafkaAvroSerializerConfig serConfig;

    public KafkaAvroDeserializerConfig getDeserConfig() {
        return deserConfig;
    }

    public KafkaAvroSerializerConfig getSerConfig() {
        return serConfig;
    }


    private static ConfigDef config;

    static {
        config = KafkaAvroDeserializerConfig.baseConfigDef()
            .define(SCHEMAS_STORAGE_LOCATION_CONFIG, ConfigDef.Type.CLASS, SCHEMAS_STORAGE_LOCATION_DEFAULT,
                ConfigDef.Importance.LOW, SCHEMAS_STORAGE_LOCATION_DOC);
    }

    public KafkaAvroSerDeConfig(Map<?, ?> props) {
        super(config, props);
        serConfig = new KafkaAvroSerializerConfig(props);
        deserConfig = new KafkaAvroDeserializerConfig(props);
    }

    public SchemaStorageClient getSchemaClient(){
        return this.getConfiguredInstance(SCHEMAS_STORAGE_LOCATION_CONFIG, SchemaStorageClient.class);
    }

    public boolean getSpecificReaderConfig(){
        return this.deserConfig.getBoolean(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG);
    }
}
