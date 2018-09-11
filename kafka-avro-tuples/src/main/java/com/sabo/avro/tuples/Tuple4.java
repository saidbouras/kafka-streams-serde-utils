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
package com.sabo.avro.tuples;

import org.apache.avro.Schema;

import java.util.Arrays;
import java.util.Collections;

/**
 * A _1/_2/_3 tuple.
 */
public class Tuple4<T1, T2, T3, T4> extends Tuple {

    public static <T1, T2, T3, T4> Tuple4<T1, T2, T3, T4> of(T1 t1, T2 t2, T3 t3, T4 t4) {
        return new Tuple4<>(t1, t2, t3, t4);
    }

    public Tuple4(Schema schema) {
        super(schema);
    }

    public Tuple4(T1 _1, T2 _2, T3 _3, T4 _4) {
        name = this.getClass().getSimpleName();
        innerTuple = Collections.unmodifiableList(Arrays.asList(_1, _2, _3, _4));
    }

    /**
     * Get the _1.
     */
    public T1 v1() {
        return (T1) innerTuple.get(0);
    }

    /**
     * Get the _2.
     */
    public T2 v2() {
        return (T2) innerTuple.get(1);
    }

    /**
     * Get the _3.
     */
    public T3 v3() {
        return (T3) innerTuple.get(2);
    }

    /**
     * Get the _4.
     */
    public T4 v4() {
        return (T4) innerTuple.get(3);
    }

    public Tuple4<T1, T2, T3, T4> as(String name) {
        this.name = Tuple.class.getSimpleName() + name;
        return this;
    }
}