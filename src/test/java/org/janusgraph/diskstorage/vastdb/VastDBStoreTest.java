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

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.janusgraph.diskstorage.vastdb.VastDBConfigOptions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic test for VAST DB backend functionality.
 * 
 * This test requires a running VAST DB instance and proper configuration.
 * Set the following environment variables:
 * - VASTDB_ENDPOINT
 * - VASTDB_AWS_ACCESS_KEY_ID
 * - VASTDB_AWS_SECRET_ACCESS_KEY
 */
public class VastDBStoreTest {
    
    private static final Logger log = LoggerFactory.getLogger(VastDBStoreTest.class);
    
    private VastDBStoreManager storeManager;
    private KeyColumnValueStore store;
    private StoreTransaction txh;
    
    @BeforeEach
    public void setUp() throws BackendException {
        Configuration config = createTestConfiguration();
        
        storeManager = new VastDBStoreManager(config);
        store = storeManager.openDatabase("test_store", StoreMetaData.EMPTY);
        txh = storeManager.beginTransaction(BaseTransactionConfig.of());
    }
    
    @AfterEach
    public void tearDown() throws BackendException {
        if (txh != null && txh.isActive()) {
            txh.commit();
        }
        
        if (store != null) {
            store.close();
        }
        
        if (storeManager != null) {
            storeManager.close();
        }
    }
    
    @Test
    public void testBasicPutAndGet() throws BackendException {
        StaticBuffer key = string2Buffer("testkey");
        StaticBuffer column = string2Buffer("testcolumn");
        StaticBuffer value = string2Buffer("testvalue");
        
        Entry entry = new Entry(column, value);
        
        // Put the entry
        store.mutate(key, Collections.singletonList(entry), Collections.emptyList(), txh);
        
        // Get the entry back
        SliceQuery query = new SliceQuery(StaticBuffer.EMPTY_BUFFER, StaticBuffer.EMPTY_BUFFER);
        KeySliceQuery keyQuery = new KeySliceQuery(key, query);
        EntryList result = store.getSlice(keyQuery, txh);
        
        // Verify the result
        assertFalse(result.isEmpty(), "Result should not be empty");
        assertEquals(1, result.size(), "Should have exactly one entry");
        
        Entry retrievedEntry = result.get(0);
        assertEquals(column, retrievedEntry.getColumn(), "Column should match");
        assertEquals(value, retrievedEntry.getValue(), "Value should match");
    }
    
    @Test
    public void testMultipleColumnsForSameKey() throws BackendException {
        StaticBuffer key = string2Buffer("multikey");
        
        Entry entry1 = new Entry(string2Buffer("column1"), string2Buffer("value1"));
        Entry entry2 = new Entry(string2Buffer("column2"), string2Buffer("value2"));
        Entry entry3 = new Entry(string2Buffer("column3"), string2Buffer("value3"));
        
        // Put multiple entries for the same key
        store.mutate(key, Arrays.asList(entry1, entry2, entry3), Collections.emptyList(), txh);
        
        // Get all entries for the key
        SliceQuery query = new SliceQuery(StaticBuffer.EMPTY_BUFFER, StaticBuffer.EMPTY_BUFFER);
        KeySliceQuery keyQuery = new KeySliceQuery(key, query);
        EntryList result = store.getSlice(keyQuery, txh);
        
        // Verify the results
        assertEquals(3, result.size(), "Should have three entries");
        
        // Entries should be ordered by column
        assertTrue(result.get(0).getColumn().compareTo(result.get(1).getColumn()) < 0);
        assertTrue(result.get(1).getColumn().compareTo(result.get(2).getColumn()) < 0);
    }
    
    @Test
    public void testSliceQuery() throws BackendException {
        StaticBuffer key = string2Buffer("slicekey");
        
        // Add entries with ordered column names
        Entry entry1 = new Entry(string2Buffer("a"), string2Buffer("value_a"));
        Entry entry2 = new Entry(string2Buffer("b"), string2Buffer("value_b"));
        Entry entry3 = new Entry(string2Buffer("c"), string2Buffer("value_c"));
        Entry entry4 = new Entry(string2Buffer("d"), string2Buffer("value_d"));
        
        store.mutate(key, Arrays.asList(entry1, entry2, entry3, entry4), Collections.emptyList(), txh);
        
        // Query for a slice: from 'b' to 'd' (exclusive)
        SliceQuery query = new SliceQuery(string2Buffer("b"), string2Buffer("d"));
        KeySliceQuery keyQuery = new KeySliceQuery(key, query);
        EntryList result = store.getSlice(keyQuery, txh);
        
        // Should get entries for columns 'b' and 'c'
        assertEquals(2, result.size(), "Should have two entries in slice");
        assertEquals(string2Buffer("b"), result.get(0).getColumn());
        assertEquals(string2Buffer("c"), result.get(1).getColumn());
    }
    
    @Test
    public void testDeleteOperations() throws BackendException {
        StaticBuffer key = string2Buffer("deletekey");
        StaticBuffer column1 = string2Buffer("col1");
        StaticBuffer column2 = string2Buffer("col2");
        
        Entry entry1 = new Entry(column1, string2Buffer("val1"));
        Entry entry2 = new Entry(column2, string2Buffer("val2"));
        
        // Add two entries
        store.mutate(key, Arrays.asList(entry1, entry2), Collections.emptyList(), txh);
        
        // Verify both are there
        SliceQuery query = new SliceQuery(StaticBuffer.EMPTY_BUFFER, StaticBuffer.EMPTY_BUFFER);
        KeySliceQuery keyQuery = new KeySliceQuery(key, query);
        EntryList result = store.getSlice(keyQuery, txh);
        assertEquals(2, result.size(), "Should have two entries before deletion");
        
        // Delete one column
        store.mutate(key, Collections.emptyList(), Collections.singletonList(column1), txh);
        
        // Verify only one remains
        result = store.getSlice(keyQuery, txh);
        assertEquals(1, result.size(), "Should have one entry after deletion");
        assertEquals(column2, result.get(0).getColumn(), "Remaining entry should be column2");
    }
    
    @Test
    public void testTransactionCommitAndRollback() throws BackendException {
        // Test transaction commit
        StoreTransaction tx1 = storeManager.beginTransaction(BaseTransactionConfig.of());
        StaticBuffer key = string2Buffer("txkey");
        Entry entry = new Entry(string2Buffer("col"), string2Buffer("val"));
        
        store.mutate(key, Collections.singletonList(entry), Collections.emptyList(), tx1);
        tx1.commit();
        
        // Verify data persisted after commit
        StoreTransaction tx2 = storeManager.beginTransaction(BaseTransactionConfig.of());
        SliceQuery query = new SliceQuery(StaticBuffer.EMPTY_BUFFER, StaticBuffer.EMPTY_BUFFER);
        KeySliceQuery keyQuery = new KeySliceQuery(key, query);
        EntryList result = store.getSlice(keyQuery, tx2);
        
        assertEquals(1, result.size(), "Entry should persist after commit");
        tx2.commit();
        
        // Test transaction rollback
        StoreTransaction tx3 = storeManager.beginTransaction(BaseTransactionConfig.of());
        Entry entry2 = new Entry(string2Buffer("col2"), string2Buffer("val2"));
        
        store.mutate(key, Collections.singletonList(entry2), Collections.emptyList(), tx3);
        tx3.rollback();
        
        // Note: Since VAST DB doesn't support true rollback, this mainly tests
        // that the transaction is properly marked as rolled back
        assertTrue(((VastDBTransaction) tx3).isRolledBack(), "Transaction should be marked as rolled back");
    }
    
    @Test
    public void testStoreFeatures() {
        var features = storeManager.getFeatures();
        
        // Verify expected features
        assertTrue(features.hasOrderedScan(), "Should support ordered scan");
        assertTrue(features.hasUnorderedScan(), "Should support unordered scan");
        assertTrue(features.isKeyOrdered(), "Keys should be ordered");
        assertTrue(features.isPersistent(), "Should be persistent");
        assertTrue(features.isKeyConsistent(), "Should be key consistent");
        assertTrue(features.hasMultiQuery(), "Should support multi-query");
        assertTrue(features.hasBatchMutation(), "Should support batch mutation");
        
        // Features that VAST DB doesn't support
        assertFalse(features.hasLocking(), "Should not support locking");
        assertFalse(features.hasCellTTL(), "Should not support cell TTL");
    }
    
    @Test
    public void testStoreManagerOperations() throws BackendException {
        // Test opening multiple stores
        KeyColumnValueStore store2 = storeManager.openDatabase("test_store2", StoreMetaData.EMPTY);
        assertNotNull(store2, "Should be able to open second store");
        
        // Test store manager name
        assertEquals("vastdb", storeManager.getName(), "Store manager name should be 'vastdb'");
        
        // Test existence check
        assertTrue(storeManager.exists(), "Store manager should exist");
        
        store2.close();
    }
    
    /**
     * Create a test configuration for VAST DB.
     * Uses environment variables for connection details.
     */
    private Configuration createTestConfiguration() {
        ModifiableConfiguration config = new ModifiableConfiguration();
        
        // Set VAST DB specific configuration
        String endpoint = System.getenv("VASTDB_ENDPOINT");
        String accessKey = System.getenv("VASTDB_AWS_ACCESS_KEY_ID");
        String secretKey = System.getenv("VASTDB_AWS_SECRET_ACCESS_KEY");
        
        if (endpoint == null || accessKey == null || secretKey == null) {
            // Use default test values if environment variables not set
            log.warn("VAST DB environment variables not set, using test defaults");
            endpoint = "http://localhost:8080";  // Default test endpoint
            accessKey = "test-access-key";
            secretKey = "test-secret-key";
        }
        
        config.set(VASTDB_ENDPOINT, endpoint);
        config.set(VASTDB_AWS_ACCESS_KEY_ID, accessKey);
        config.set(VASTDB_AWS_SECRET_ACCESS_KEY, secretKey);
        config.set(VASTDB_KEYSPACE, "test_keyspace");
        config.set(VASTDB_AUTO_CREATE_TABLES, true);
        config.set(VASTDB_BATCH_SIZE, 100);
        config.set(VASTDB_CONNECTION_TIMEOUT_MS, 30000);
        config.set(VASTDB_READ_TIMEOUT_MS, 60000);
        
        return new BasicConfiguration(GraphDatabaseConfiguration.ROOT_NS, config, BasicConfiguration.Restriction.NONE);
    }
    
    /**
     * Convert a string to a StaticBuffer for testing.
     */
    private StaticBuffer string2Buffer(String s) {
        return new StaticArrayBuffer(s.getBytes(StandardCharsets.UTF_8));
    }
}