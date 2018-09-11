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
package com.sabo.examples;

import com.google.common.collect.ImmutableList;
import com.sabo.utils.TestDatasExamples;
import com.sabo.schemas.Adress;
import com.sabo.schemas.CustomerId;
import com.sabo.serdes.avro.GenericAvroSerializer;
import com.sabo.serdes.avro.SpecificAvroSerializer;
import com.sabo.serdes.avro.LocalGenericSerde;
import com.sabo.serdes.avro.LocalSpecificSerde;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.Test;
import com.sabo.utils.KafkaBaseIT;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.sabo.examples.ExampleInternalSerde.*;
import static org.assertj.core.api.Assertions.assertThat;

public class ExampleInternalSerdeITest extends KafkaBaseIT implements TestDatasExamples {

    private static final String APP_SPECIFIC_ID = "specific-record-schema-internal-topics";
    private static final String APP_GENERIC_ID = "generic-record-schema-internal-topics";

    private ExampleInternalSerde appStream =  new ExampleInternalSerde();

    @Override
    public String[] getTopics() {
        return new String[]{
            INPUT_CUSTOMERS_V1_TOPIC, INPUT_CUSTOMERS_V2_TOPIC,
            OUTPUT_ADRESS_V1_TOPIC, OUTPUT_ADRESS_V2_TOPIC
        };
    }

    @Test
    public void should_use_internal_serde_with_specific_record_on_internal_topics() throws ExecutionException, InterruptedException, IOException, RestClientException {
        final List<String> expectedSubjects = ImmutableList.of(
            INPUT_CUSTOMERS_V1_TOPIC + "-value", INPUT_CUSTOMERS_V1_TOPIC + "-key",
            OUTPUT_ADRESS_V1_TOPIC + "-value", OUTPUT_ADRESS_V1_TOPIC + "-key"
        );

        final Properties streamsConfiguration = createStreamProperties(APP_SPECIFIC_ID,
            CLUSTER.bootstrapServers(), CLUSTER.schemaRegistryUrl(),
            LocalSpecificSerde.class, LocalSpecificSerde.class
        );

        final StreamsBuilder builder = appStream.createSpecificStreamTopology(streamsConfiguration);
        streams = new KafkaStreams(builder.build(), streamsConfiguration);
        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        startStreamApp(streams);

        produce(INPUT_CUSTOMERS_V1_TOPIC,
            ImmutableList.of(KeyValue.pair(new CustomerId("00871"), buildSpecificCustomer())),
            SpecificAvroSerializer.class, SpecificAvroSerializer.class
        );

        List<KeyValue<Adress, Adress>> results = consumeSpecific(APP_SPECIFIC_ID, OUTPUT_ADRESS_V1_TOPIC, 1);

        assertThat(results).isNotEmpty();
        List<String> allSubjects = CLUSTER.getAllSubjects();
        assertThat(allSubjects).containsOnlyElementsOf(expectedSubjects);
    }


    @Test
    public void should_use_internal_serde_with_generic_record_on_internal_topics() throws ExecutionException, InterruptedException, IOException, RestClientException {
        final List<String> expectedSubjects = ImmutableList.of(
            INPUT_CUSTOMERS_V2_TOPIC + "-value", INPUT_CUSTOMERS_V2_TOPIC + "-key",
            OUTPUT_ADRESS_V2_TOPIC + "-value", OUTPUT_ADRESS_V2_TOPIC + "-key"
        );

        final Properties streamsConfiguration = createStreamProperties(APP_GENERIC_ID,
            CLUSTER.bootstrapServers(), CLUSTER.schemaRegistryUrl(),
            LocalGenericSerde.class, LocalGenericSerde.class
        );

        final StreamsBuilder builder = appStream.createGenericStreamTopology(streamsConfiguration);
        streams = new KafkaStreams(builder.build(), streamsConfiguration);
        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        startStreamApp(streams);

        GenericData.Record key = new GenericData.Record(CustomerId.SCHEMA$);
        key.put("id", "00872");
        ImmutableList<KeyValue<GenericRecord, GenericRecord>> inputsValue =
            ImmutableList.of(KeyValue.pair(key, buildGenericCustomer()));

        produce(INPUT_CUSTOMERS_V2_TOPIC, inputsValue, GenericAvroSerializer.class, GenericAvroSerializer.class);

        List<KeyValue<Object, Object>> results = consumeGeneric(APP_GENERIC_ID, OUTPUT_ADRESS_V2_TOPIC, 1);

        assertThat(results).isNotEmpty();
        List<String> allSubjects = CLUSTER.getAllSubjects();
        assertThat(allSubjects).containsOnlyElementsOf(expectedSubjects);
    }
}
