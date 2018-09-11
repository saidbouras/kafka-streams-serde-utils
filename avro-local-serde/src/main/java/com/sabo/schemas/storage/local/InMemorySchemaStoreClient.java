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
package com.sabo.schemas.storage.local;

import com.sabo.client.AbstractSchemaStorageClient;
import com.sabo.stores.exceptions.SchemaStorageException;
import com.sabo.stores.registry.AvroSchemaStore;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import org.apache.avro.Schema;

import java.io.IOException;

public class InMemorySchemaStoreClient extends AbstractSchemaStorageClient {

    private static final Integer DEFAULT_SCHEMA_VERSION = 1;
    private static final int DEFAULT_ID = 1;

    private final AvroSchemaStore schemaStore;

    public InMemorySchemaStoreClient() {
        keySubjectNameStrategy = new TopicNameStrategy();
        valueSubjectNameStrategy = new TopicNameStrategy();
        schemaStore = LocalStorageManager.localInMemorySchemaRegistry().get();
    }


    public Schema getSchema(String subject) throws IOException, RestClientException {
        try {
            // A bit messy, the schema registry can return null, I guess the store can return the same
            return schemaStore.get(subject).orElse(null);
        } catch (SchemaStorageException e) {
            throw new IllegalStateException("A problem occured when trying to retrieve the schema for " +
                "the subject " + subject);
        }
    }

    @Override
    public Schema getSchema(String topic, boolean isKey, Object value, int id, boolean includeSchemaAndVersion) throws IOException, RestClientException {
        final String subjectName = getSubjectName(topic, isKey, value);
        return getSchema(subjectName);
    }

    @Override
    public int register(String subject, Schema schema) throws IOException, RestClientException {
        try {
            return schemaStore.register(subject, schema);
        } catch (SchemaStorageException e) {
            throw new IllegalStateException("A problem occured when trying to register the schema " + schema.getFullName()
                + " under the subject : " + subject);
        }
    }

    @Override
    public Integer getSchemaVersion(String subject, Schema subjectSchema) throws IOException, RestClientException {
        return DEFAULT_SCHEMA_VERSION;
    }

    @Override
    public int getId(String subject, Schema schema) throws IOException, RestClientException {
        return DEFAULT_ID;
    }

    @Override
    public void configureClientProperties(AbstractKafkaAvroSerDeConfig config) {
        // do nothing
    }
}
