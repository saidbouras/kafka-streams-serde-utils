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
package com.sabo.schemas.storage.local;

import com.sabo.stores.InMemoryStore;
import com.sabo.stores.Store;
import com.sabo.stores.commons.Utils;
import com.sabo.stores.exceptions.SchemaStorageException;
import com.sabo.stores.exceptions.StoreException;
import com.sabo.stores.registry.AvroSchemaStore;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

public class LocalInMemorySchemaStore implements AvroSchemaStore {

    private static final Logger log = LogManager.getLogger(LocalInMemorySchemaStore.class);

    private final Store<String, Schema> innerStore = new InMemoryStore<>();

    @Override
    public void init() {
        // do nothing
    }

    @Override
    public int register(String subject, Schema schema) {
        try {
            innerStore.put(subject, schema);
        } catch (StoreException e) {
            log.error("An error occurs while trying to register schema : {} under subject : {}", schema, subject);
        }
        return 1;
    }

    @Override
    public Optional<Schema> get(String subject) throws SchemaStorageException {
        try {
            return Optional.ofNullable(innerStore.get(subject));
        } catch (StoreException e) {
            throw new SchemaStorageException("Not found schema under subject : " + subject, 404, 404);
        }
    }

    @Override
    public Set<String> listSubjects() {
        try {
            return Utils.toSet(innerStore.getAllKeys());
        } catch (StoreException e) {
            log.error("An error occurs when trying to retrieve all schemas");
        }
        return Collections.emptySet();
    }


    @Override
    public void deleteSubject(String subject) {
        try {
            innerStore.delete(subject);
        } catch (StoreException e) {
            log.error("An error occurs when trying to delete the schema under subject : {}", subject);
        }
    }

    @Override
    public void close() {
        innerStore.close();
    }
}
