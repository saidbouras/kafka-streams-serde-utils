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

import com.sabo.schemas.Adress;
import com.sabo.schemas.Customer;
import com.sabo.schemas.CustomerId;
import com.sabo.schemas.Orders;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public interface TestDatasExamples {

    default Customer buildSpecificCustomer() {
        return Customer.newBuilder()
                .setName("Jon")
                .setSurname("Snow")
                .setSocialNumber(8761236L)
                .setAdress(buildSpecificAdress())
                .build();
    }

    default GenericRecord buildGenericCustomer() {
        GenericData.Record genericRecord = new GenericData.Record(Customer.SCHEMA$);
        genericRecord.put("name", "Jon");
        genericRecord.put("surname", "Snow");
        genericRecord.put("social_number", 8763236L);
        genericRecord.put("adress", buildGenericAdress());

        return genericRecord;
    }

    default GenericData.Record buildGenericAdress() {
        GenericData.Record record = new GenericData.Record(Adress.SCHEMA$);
        record.put("line_one", "jaguar street");
        record.put("number", 456L);
        record.put("postal_number", 75020L);
        return record;
    }

    default Adress buildSpecificAdress() {
        return Adress.newBuilder()
                .setLineOne("hornets avenue")
                .setNumber(187L)
                .setPostalNumber(87699L)
                .build();
    }

    default Orders makeOrders(Long orderId) {
        return Orders.newBuilder()
                .setCustomerId(new CustomerId("00871"))
                .setOrderId(orderId)
                .setDetails("Dummy texts")
                .build();
    }
}
