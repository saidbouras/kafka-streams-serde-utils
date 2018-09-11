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
package com.sabo.utils;

import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import org.apache.curator.test.InstanceSpec;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Runs an in-memory, "embedded" Kafka cluster with :
 * 1 ZooKeeper instance
 * 1 Kafka broker
 * 1 schema registry instance
 */
public class EmbeddedSingleNodeKafkaCluster extends EmbeddedKafkaCluster {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedSingleNodeKafkaCluster.class);
    private static final String KAFKA_SCHEMAS_TOPIC = "_schemas";
    private static final String AVRO_COMPATIBILITY_TYPE = AvroCompatibilityLevel.BACKWARD.name;

    private RestApp schemaRegistry;

    /**
     * Creates and starts a Kafka cluster.
     */
    public EmbeddedSingleNodeKafkaCluster() {
        super(1, new Properties());
    }

    /**
     * Creates and starts a Kafka cluster.
     */
    @Override
    protected void before() throws Throwable {
        super.before();
        startSchemaRegistry();
    }

    @Override
    protected void after() {
        stopSchemaRegistry();
        // Stop Kafka and Zookeeper
        super.after();
    }

    private void startSchemaRegistry() {
        Properties schemaRegistryProperties = new Properties();
        schemaRegistryProperties.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_REPLICATION_FACTOR_CONFIG, 1);

        schemaRegistry = new RestApp(
                InstanceSpec.getRandomPort(),
                zKConnectString(),
                KAFKA_SCHEMAS_TOPIC,
                AVRO_COMPATIBILITY_TYPE, schemaRegistryProperties
        );
        try {
            /* start schema registry */
            schemaRegistry.start();
        } catch (InvalidReplicationFactorException e) {
            log.error("Replication factor error", e);
        } catch (Exception e) {
            log.error("Error while starting client", e);
        }
    }

    public List<String> getAllSubjects() throws IOException, RestClientException {
        return schemaRegistry.restClient.getAllSubjects();
    }

    public Schema getSchema(String subject) throws IOException, RestClientException {
        return schemaRegistry.restClient.getLatestVersion(subject);
    }

    public void deleteSubjects(List<String> subjects) {
        subjects.forEach(subject -> {
            try {
                schemaRegistry.restApp.schemaRegistry().deleteSubject(subject);
            } catch (SchemaRegistryException e) {
                e.printStackTrace();
            }
        });
    }

    public void deleteSubjects(String ...subjects) {
        deleteSubjects(Arrays.asList(subjects));
    }

    protected void stopSchemaRegistry() {
        try {
            if (schemaRegistry != null) {
                schemaRegistry.stop();
            }
        } catch (Exception e) {
            log.error("Error while stopping client and kafka rest", e);
        }
    }

    /**
     * The "schema.registry.url" setting of this schema registry instance.
     */
    public String schemaRegistryUrl() {
        return schemaRegistry.restConnect;
    }
}
