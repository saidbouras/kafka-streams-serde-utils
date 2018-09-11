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
package com.sabo.stores;

import com.sabo.stores.exceptions.StoreException;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryStore<K, V> implements Store<K, V> {

    private final ConcurrentSkipListMap<K, V> innerStore;

    public InMemoryStore() {
        innerStore = new ConcurrentSkipListMap<>();
    }

    @Override
    public V get(K key) throws StoreException {
        return innerStore.get(key);
    }

    @Override
    public void put(K key, V value) throws StoreException {
        innerStore.put(key, value);
    }

    @Override
    public void putAll(Map<K, V> entries) throws StoreException {
        innerStore.putAll(entries);
    }

    @Override
    public V delete(K key) throws StoreException {
        return innerStore.remove(key);
    }

    @Override
    public Iterator<K> getAllKeys() throws StoreException {
        return innerStore.keySet().iterator();
    }

    @Override
    public void close() {
        innerStore.clear();
    }
}