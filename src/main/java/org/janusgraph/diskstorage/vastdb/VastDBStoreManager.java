// Copyright 2024 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.vastdb;

import com.google.common.base.Preconditions;
import com.vastdata.vdb.sdk.VastSdk;
import com.vastdata.vdb.sdk.VastSdkConfig;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.common.AbstractStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.janusgraph.diskstorage.vastdb.VastDBConfigOptions.*;

public class VastDBStoreManager extends AbstractStoreManager implements KeyColumnValueStoreManager {
    
    private static final Logger log = LoggerFactory.getLogger(VastDBStoreManager.class);
    
    public static final String VASTDB_BACKEND = "vastdb";
    
    private final Map<String, VastDBKeyColumnValueStore> stores = new ConcurrentHashMap<>();
    private final StandardStoreFeatures features;
    
    private final VastSdk vastSdk;
    private final String keyspace;
    
    public VastDBStoreManager(Configuration configuration) throws BackendException {
        super(configuration);
        
        this.keyspace = configuration.get(VASTDB_KEYSPACE);
        
        // Initialize VAST SDK
        String endpoint = configuration.get(VASTDB_ENDPOINT);
        String awsAccessKeyId = configuration.get(VASTDB_AWS_ACCESS_KEY_ID);
        String awsSecretAccessKey = configuration.get(VASTDB_AWS_SECRET_ACCESS_KEY);
        
        Preconditions.checkNotNull(endpoint, "VAST DB endpoint must be specified");
        Preconditions.checkNotNull(awsAccessKeyId, "AWS access key ID must be specified");
        Preconditions.checkNotNull(awsSecretAccessKey, "AWS secret access key must be specified");
        
        try {
            URI uri = URI.create(endpoint);
            VastSdkConfig config = new VastSdkConfig(uri, uri.toString(), awsAccessKeyId, awsSecretAccessKey);
            HttpClient httpClient = new JettyHttpClient();
            this.vastSdk = new VastSdk(httpClient, config);
            
            // Initialize keyspace/schema if it doesn't exist
            initializeKeyspace();
            
        } catch (Exception e) {
            throw new PermanentBackendException("Failed to initialize VAST DB connection", e);
        }
        
        this.features = new StandardStoreFeatures.Builder()
            .orderedScan(true)
            .unorderedScan(true)
            .keyOrdered(true)
            .persists(true)
            .optimisticLocking(configuration.get(VASTDB_ENABLE_OPTIMISTIC_LOCKING))
            .keyConsistent(true)
            .locking(false)  // VAST DB doesn't support distributed locking
            .txIsolation(configuration.get(VASTDB_ISOLATION_LEVEL))
            .supportsInterruption(false)
            .cellTTL(false)  // VAST DB doesn't natively support TTL
            .timestamps(true)
            .preferredTimestamps(PREFERRED_TIMESTAMPS)
            .multiQuery(true)
            .batchMutation(true)
            .localKeyPartition(false)
            .build();
    }
    
    private void initializeKeyspace() throws BackendException {
        try {
            // Create schema/keyspace in VAST DB if it doesn't exist
            // This would use the internal VAST client API similar to the example
            log.info("Initializing VAST DB keyspace: {}", keyspace);
            // Implementation would create the schema using VastClient if needed
        } catch (Exception e) {
            throw new PermanentBackendException("Failed to initialize keyspace: " + keyspace, e);
        }
    }
    
    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        return new VastDBTransaction(config);
    }
    
    @Override
    public void close() throws BackendException {
        try {
            // Close all stores
            for (VastDBKeyColumnValueStore store : stores.values()) {
                store.close();
            }
            stores.clear();
            
            log.info("Closed VAST DB store manager");
        } catch (Exception e) {
            throw new PermanentBackendException("Failed to close VAST DB store manager", e);
        }
    }
    
    @Override
    public void clearStorage() throws BackendException {
        try {
            log.warn("Clearing all data in VAST DB keyspace: {}", keyspace);
            
            // Drop all tables in the keyspace
            for (String storeName : stores.keySet()) {
                dropTable(storeName);
            }
            stores.clear();
            
        } catch (Exception e) {
            throw new PermanentBackendException("Failed to clear storage", e);
        }
    }
    
    @Override
    public boolean exists() throws BackendException {
        try {
            // Check if keyspace exists in VAST DB
            return true; // Simplified - would check if keyspace exists
        } catch (Exception e) {
            throw new PermanentBackendException("Failed to check if keyspace exists", e);
        }
    }
    
    @Override
    public StoreFeatures getFeatures() {
        return features;
    }
    
    @Override
    public String getName() {
        return VASTDB_BACKEND;
    }
    
    @Override
    public KeyColumnValueStore openDatabase(String name) throws BackendException {
        Preconditions.checkNotNull(name);
        
        if (stores.containsKey(name)) {
            return stores.get(name);
        }
        
        try {
            VastDBKeyColumnValueStore store = new VastDBKeyColumnValueStore(
                vastSdk, 
                keyspace, 
                name, 
                storageConfig,
                this
            );
            
            stores.put(name, store);
            return store;
            
        } catch (Exception e) {
            throw new PermanentBackendException("Failed to open store: " + name, e);
        }
    }
    
    @Override
    public List<KeyColumnValueStore> openDatabases(String... names) throws BackendException {
        List<KeyColumnValueStore> stores = new java.util.ArrayList<>();
        for (String name : names) {
            stores.add(openDatabase(name));
        }
        return stores;
    }
    
    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, org.janusgraph.diskstorage.keycolumnvalue.KCVMutation>> mutations, 
                          StoreTransaction txh) throws BackendException {
        // Batch mutations across multiple stores
        try {
            for (Map.Entry<String, Map<StaticBuffer, org.janusgraph.diskstorage.keycolumnvalue.KCVMutation>> storeEntry : mutations.entrySet()) {
                String storeName = storeEntry.getKey();
                Map<StaticBuffer, org.janusgraph.diskstorage.keycolumnvalue.KCVMutation> storeMutations = storeEntry.getValue();
                
                KeyColumnValueStore store = openDatabase(storeName);
                for (Map.Entry<StaticBuffer, org.janusgraph.diskstorage.keycolumnvalue.KCVMutation> mutation : storeMutations.entrySet()) {
                    StaticBuffer key = mutation.getKey();
                    org.janusgraph.diskstorage.keycolumnvalue.KCVMutation kcvMutation = mutation.getValue();
                    
                    if (kcvMutation.hasAdditions()) {
                        store.mutate(key, kcvMutation.getAdditions(), java.util.Collections.emptyList(), txh);
                    }
                    if (kcvMutation.hasDeletions()) {
                        store.mutate(key, java.util.Collections.emptyList(), kcvMutation.getDeletions(), txh);
                    }
                }
            }
        } catch (Exception e) {
            throw new PermanentBackendException("Failed to execute batch mutations", e);
        }
    }
    
    private void dropTable(String tableName) throws BackendException {
        try {
            log.info("Dropping table: {}/{}", keyspace, tableName);
            // Implementation would drop the table using VastClient
        } catch (Exception e) {
            throw new PermanentBackendException("Failed to drop table: " + tableName, e);
        }
    }
    
    public VastSdk getVastSdk() {
        return vastSdk;
    }
    
    public String getKeyspace() {
        return keyspace;
    }
}