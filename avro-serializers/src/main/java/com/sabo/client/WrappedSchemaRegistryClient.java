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
package com.sabo.client;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import org.apache.avro.Schema;
import org.apache.kafka.common.config.ConfigException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WrappedSchemaRegistryClient extends AbstractSchemaStorageClient {

    private SchemaRegistryClient client;

    public WrappedSchemaRegistryClient() {
        keySubjectNameStrategy = new TopicNameStrategy();
        valueSubjectNameStrategy = new TopicNameStrategy();
    }

    public WrappedSchemaRegistryClient(SchemaRegistryClient client) {
        this.client = client;
    }


    @Override
    public Schema getSchema(String topic, boolean isKey, Object value, int id, boolean includeSchemaAndVersion) throws IOException, RestClientException{
        String subject = includeSchemaAndVersion ? getSubjectName(topic, isKey, null) : null;
        return client.getBySubjectAndId(subject, id);
    }

    @Override
    public Integer getSchemaVersion(String subject, Schema subjectSchema) throws IOException, RestClientException {
        return client.getVersion(subject, subjectSchema);
    }

    @Override
    public int register(String subject, Schema schema) throws IOException, RestClientException {
        return client.register(subject, schema);
    }

    @Override
    public int getId(String subject, Schema schema) throws IOException, RestClientException {
        return client.getId(subject, schema);
    }

    @Override
    public void configureClientProperties(AbstractKafkaAvroSerDeConfig config) {
        try {
            List<String> urls = config.getSchemaRegistryUrls();
            int maxSchemaObject = config.getMaxSchemasPerSubject();
            Map<String, Object> originals = config.originalsWithPrefix("");
            if (null == client) {
                client = new CachedSchemaRegistryClient(urls, maxSchemaObject, originals);
            }
            keySubjectNameStrategy = config.keySubjectNameStrategy();
            valueSubjectNameStrategy = config.valueSubjectNameStrategy();
        } catch (io.confluent.common.config.ConfigException e) {
            throw new ConfigException(e.getMessage());
        }
    }
}
