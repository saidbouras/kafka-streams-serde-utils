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

import com.sabo.serdes.avro.GenericAvroDeserializer;
import com.sabo.serdes.avro.SpecificAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public abstract class KafkaBaseIT {

    private static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    protected static Properties properties;

    protected KafkaStreams streams;

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    @BeforeClass
    public static void initialize() {
        properties = new Properties();
        properties.put(SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
    }

    @Before
    public void setUp() throws Exception {
        CLUSTER.createTopics(getTopics());
    }

    @After
    public void reset() throws Exception {
        if (KafkaStreams.State.RUNNING == streams.state()) {
            streams.close();
        }
        CLUSTER.deleteTopicsAndWait(getTopics());
        CLUSTER.deleteSubjects(CLUSTER.getAllSubjects());
    }

    public abstract String[] getTopics();

    protected void startStreamApp(KafkaStreams streams) {
        streams.cleanUp();
        streams.start();
    }


    protected <K, V, SK extends Serializer, SV extends Serializer>
    void produce(String topic, List<KeyValue<K, V>> data, Class<SK> keySer, Class<SV> valueSer) throws ExecutionException, InterruptedException {
        final Properties producerConfig = TestUtils.producerConfig(
            CLUSTER.bootstrapServers(),
            keySer,
            valueSer,
            properties
        );

        IntegrationTestUtils.produceKeyValuesSynchronously(
            topic,
            data,
            producerConfig,
            Time.SYSTEM
        );
    }


    protected <K, V, SK extends Deserializer, SV extends Deserializer> List<KeyValue<K, V>>
    consume(String appId, String topic, int numRecords, Class<SK> keyDeSer, Class<SV> valDeSer) throws InterruptedException {
        final Properties consumerConfig = TestUtils.consumerConfig(
            CLUSTER.bootstrapServers(), appId + "-consumer-test",
            keyDeSer,
            valDeSer,
            properties);

        return IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            consumerConfig,
            topic,
            numRecords,
            30 * 1000
        );
    }

    protected <K, V> List<KeyValue<K, V>> consumeGeneric(String appId, String topic, int numRecords) throws InterruptedException {
        return consume(appId + "-generic", topic, numRecords, GenericAvroDeserializer.class, GenericAvroDeserializer.class);
    }

    protected <K, V> List<KeyValue<K, V>> consumeSpecific(String appId, String topic, int numRecords) throws InterruptedException {
        return consume(appId + "-specific", topic, numRecords,
            SpecificAvroDeserializer.class,
            SpecificAvroDeserializer.class);
    }

    protected <K extends Serde, V extends Serde> Properties createStreamProperties(String appIdPrefix, String bootstrapServers, String schemaRegistryUrl,
                                                                                   Class<K> keySerde, Class<V> valueSerde) {
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appIdPrefix + "-example");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, appIdPrefix + "-example-client");

        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Where to find the Confluent schema registry instance(s)
        streamsConfiguration.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerde);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerde);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
//        streamsConfiguration.put(KafkaAvroSerDeConfig.SCHEMAS_STORAGE_LOCATION_CONFIG, )
        return streamsConfiguration;
    }
}
