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

import com.sabo.schemas.Adress;
import com.sabo.schemas.Customer;
import com.sabo.schemas.CustomerId;
import com.sabo.serde.SerdeUtils;
import com.sabo.serdes.avro.GenericAvroSerde;
import com.sabo.serdes.avro.SpecificAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class ExampleInternalSerde {

    public static final String INPUT_CUSTOMERS_V1_TOPIC = "customers-v1-topic";
    public static final String INPUT_CUSTOMERS_V2_TOPIC = "customers-v2-topic";
    public static final String OUTPUT_ADRESS_V1_TOPIC = "adress-v1-topic";
    public static final String OUTPUT_ADRESS_V2_TOPIC = "adress-v2-topic";


    public  StreamsBuilder createSpecificStreamTopology(Properties properties) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<CustomerId, Customer> specificCustomerStream = streamsBuilder.stream(INPUT_CUSTOMERS_V1_TOPIC,
                SerdeUtils.consumedWith(properties, new SpecificAvroSerde<>(), new SpecificAvroSerde<>()));

        specificCustomerStream
                .groupBy((id, customer) -> customer.getAdress())
                .aggregate(
                        () -> Adress.newBuilder().build(),
                        (key, value, agg) -> value.getAdress()
                )
                .toStream()
                .to(OUTPUT_ADRESS_V1_TOPIC,
                        SerdeUtils.producedWith(properties, new SpecificAvroSerde<>(), new SpecificAvroSerde<>()));

        return streamsBuilder;
    }

    public StreamsBuilder createGenericStreamTopology(Properties properties) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<GenericRecord, GenericRecord> genericCustomerStream = streamsBuilder.stream(INPUT_CUSTOMERS_V2_TOPIC,
                SerdeUtils.consumedWith(properties, new GenericAvroSerde(), new GenericAvroSerde()));

        KTable<GenericRecord, GenericRecord> aggregate = genericCustomerStream
                .groupBy((id, customer) -> ((GenericRecord) customer.get("adress")))
                .aggregate(
                        () -> null,
                        (key, value, agg) -> {
                            GenericRecord adress = (GenericRecord) value.get("adress");
                            return adress;
                        }
                );

        aggregate.toStream()
                .to(OUTPUT_ADRESS_V2_TOPIC,
                        SerdeUtils.producedWith(properties, new GenericAvroSerde(), new GenericAvroSerde()));

        return streamsBuilder;
    }
}
