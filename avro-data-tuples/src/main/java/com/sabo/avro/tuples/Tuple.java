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
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class Tuple extends SpecificRecordBase implements SpecificData.SchemaConstructable {

    protected Schema schema;
    protected String name;
    protected List<Object> innerTuple;

    public Tuple() {
        name = this.getClass().getSimpleName();
        innerTuple = new ArrayList<>();
    }

    public Tuple(Schema schema){
        if(!SpecificTuplesData.isTuples(schema)){
            throw new IllegalArgumentException("This is not a valid tuples");
        }
        this.schema = schema;
        this.name = schema.getName();
        this.innerTuple = new ArrayList<>();
    }

    @Override
    public Schema getSchema() {
        if (schema == null) {
            List<Schema> schemas = innerTuple.stream()
                .map(SpecificTuplesData.get()::getSchema)
                .collect(Collectors.toList());

            this.schema = SpecificTuplesData.get().makeTupleSchemas(name, schemas);
        }
        return schema;
    }

    @Override
    public Object get(int field) {
        return innerTuple.get(field);
    }

    @Override
    public void put(int field, Object value) {
        innerTuple.add(field, value);
    }
}