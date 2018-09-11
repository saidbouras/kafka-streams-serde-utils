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
import com.sabo.schemas.Orders;
import com.sabo.serde.SerdeUtils;
import com.sabo.serdes.avro.SpecificAvroSerde;
import com.sabo.avro.tuples.Tuple11;
import com.sabo.avro.tuples.Tuple2;
import com.sabo.avro.tuples.Tuple3;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class ExampleWithTuples {

    public static final String INPUT_CUSTOMERS_TUPLE_2 = "input-customers-tuple2";
    public static final String INPUT_ORDERS_TUPLE_2 = "input-orders-tuple2";
    public static final String OUTPUT_TUPLE_2 = "output-tuple2";
    public static final String OUTPUT_WITH_ADRESS_TUPLE_2 = "output-adress-tuple2";

    public StreamsBuilder createTopologyTuple2(Properties properties) {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<CustomerId, Customer> customerStream = streamsBuilder.stream(INPUT_CUSTOMERS_TUPLE_2);

        KStream<Long, Orders> ordersStream = streamsBuilder.stream(INPUT_ORDERS_TUPLE_2,
                SerdeUtils.consumedWith(properties, Serdes.Long(), new SpecificAvroSerde<>()));

        KTable<CustomerId, Long> ordersCount = ordersStream
                .selectKey(((key, value) -> value.getCustomerId()))
                .groupByKey()
                .count();

        final KStream<CustomerId, Tuple11<Customer, Long, Adress, Integer, Integer, Integer, Integer, Orders, String, String, String>> pairWithAdress = customerStream
                .join(ordersCount, Tuple2::of)
                .through(OUTPUT_TUPLE_2)
                .mapValues(pair -> Tuple3.of(pair.v1(), pair.v2(), Adress.newBuilder().setNumber(45674L).build()))
                .mapValues(triple ->
                        Tuple11.of(triple.v1(), triple.v2(), triple.v3(), 4, 5, 6,
                                7, Orders.newBuilder().setOrderId(56L).build(), "Voila", "Le", "Tuple11").as("Triple11"));

        pairWithAdress
                .to(OUTPUT_WITH_ADRESS_TUPLE_2);

        return streamsBuilder;
    }
}