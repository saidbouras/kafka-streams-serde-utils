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
import com.sabo.schemas.Customer;
import com.sabo.schemas.CustomerId;
import com.sabo.schemas.Orders;
import com.sabo.serdes.avro.SpecificAvroDeserializer;
import com.sabo.serdes.avro.SpecificAvroSerde;
import com.sabo.serdes.avro.SpecificAvroSerializer;
import com.sabo.avro.tuples.Tuple2;
import com.sabo.avro.tuples.Tuple3;
import com.sabo.avro.tuples.Tuple11;
import com.sabo.utils.KafkaBaseIT;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.Test;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.sabo.examples.ExampleWithTuples.*;
import static org.assertj.core.api.Assertions.assertThat;

public class ExampleWithTuplesITest extends KafkaBaseIT implements TestDatasExamples {

    private ExampleWithTuples appStream = new ExampleWithTuples();

    @Override
    public String[] getTopics() {
        return new String[]{
            INPUT_CUSTOMERS_TUPLE_2, INPUT_ORDERS_TUPLE_2,
            OUTPUT_TUPLE_2, OUTPUT_WITH_ADRESS_TUPLE_2
        };
    }

    @Test
    public void should_serialize_tuple2_in_avro() throws ExecutionException, InterruptedException, IOException, RestClientException {
        String appIdPrefix = "tuple2-app-stream";
        final Properties streamsConfiguration = createStreamProperties(appIdPrefix,
            CLUSTER.bootstrapServers(), CLUSTER.schemaRegistryUrl(),
            SpecificAvroSerde.class,
            SpecificAvroSerde.class
        );

        final StreamsBuilder builder = appStream.createTopologyTuple2(streamsConfiguration);
        streams = new KafkaStreams(builder.build(), streamsConfiguration);
        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        startStreamApp(streams);

        long key = 13871L;
        Orders value = makeOrders(key);
        produce(INPUT_ORDERS_TUPLE_2, ImmutableList.of(KeyValue.pair(key, value)),
            LongSerializer.class, SpecificAvroSerializer.class
        );

        Utils.sleep(2000L);

        produce(INPUT_CUSTOMERS_TUPLE_2,
            ImmutableList.of(KeyValue.pair(new CustomerId("00871"), buildSpecificCustomer())),
            SpecificAvroSerializer.class,
            SpecificAvroSerializer.class
        );

        final List<KeyValue<CustomerId, Tuple2<Customer, Orders>>> output =
            consume(appIdPrefix, OUTPUT_TUPLE_2, 1,
                SpecificAvroDeserializer.class,
                SpecificAvroDeserializer.class);

        final List<KeyValue<CustomerId, Tuple2<Tuple3<Customer, Long, Adress>, Long>>> outputWithAdress =
            consume(appIdPrefix, OUTPUT_WITH_ADRESS_TUPLE_2, 1,
                SpecificAvroDeserializer.class,
                SpecificAvroDeserializer.class);

        assertThat(output).hasSize(1);
        assertThat(outputWithAdress).hasSize(1);
        assertThat(output.get(0).value).isExactlyInstanceOf(Tuple2.class);
        assertThat(outputWithAdress.get(0).value).isExactlyInstanceOf(Tuple11.class);

//        String outputSchema = CLUSTER.getSchema(OUTPUT_TUPLE_2 + "value").getSchema();
//        String outputWithAdressSchema = CLUSTER.getSchema(OUTPUT_WITH_ADRESS_TUPLE_2 + "value").getSchema();

    }
}