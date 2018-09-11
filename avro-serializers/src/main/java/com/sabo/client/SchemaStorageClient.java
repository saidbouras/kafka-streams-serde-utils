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

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;

import java.io.IOException;

public interface SchemaStorageClient {
    /**
     * Get the subject name for the given topic and value type.
     */
    String getSubjectName(String topic, boolean isKey, Object value);

    Schema getSchema(String topic, boolean isKey, Object value, int id, boolean includeSchemaAndVersion) throws IOException, RestClientException;

    Integer getSchemaVersion(String subject, Schema subjectSchema) throws IOException, RestClientException;

    int register(String subject, Schema schema) throws IOException, RestClientException;

    int getId(String subject, Schema schema) throws IOException, RestClientException;

    void configureClientProperties(AbstractKafkaAvroSerDeConfig config);
}
