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
package com.sabo.serde;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;

public class SerdeUtils {

    @SuppressWarnings("unchecked")
    private static <K, V> void configureSerdes(Map properties, Serde<K> keySerde, Serde<V> valueSerde) {
        keySerde.configure(properties, true);
        valueSerde.configure(properties, false);
    }

    public static <K, V> Consumed<K, V> consumedWith(Map properties, Serde<K> keySerde, Serde<V> valueSerde) {
        configureSerdes(properties, keySerde, valueSerde);
        return Consumed.with(keySerde, valueSerde);
    }

    public static <K, V> Produced<K, V> producedWith(Map properties, Serde<K> keySerde, Serde<V> valueSerde) {
        configureSerdes(properties, keySerde, valueSerde);
        return Produced.with(keySerde, valueSerde);
    }

    public static <K, V> Consumed<K, V> consumedWith(StreamsConfig streamsConfig, Serde<K> keySerde, Serde<V> valueSerde) {
        return consumedWith(streamsConfig.originals(), keySerde, valueSerde);
    }

    public static <K, V> Produced<K, V> producedWith(StreamsConfig streamsConfig, Serde<K> keySerde, Serde<V> valueSerde) {
        return producedWith(streamsConfig.originals(), keySerde, valueSerde);
    }
}