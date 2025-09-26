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

import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

import static org.janusgraph.diskstorage.vastdb.VastDBConfigOptions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for VAST DB backend components.
 * 
 * These tests focus on testing individual components without requiring
 * a full VAST DB connection or complex setup.
 */
public class VastDBStoreTest {
    
    private static final Logger log = LoggerFactory.getLogger(VastDBStoreTest.class);
    
    @Test
    public void testConfigurationOptions() {
        // Test that all configuration options are properly defined
        assertNotNull(VASTDB_ENDPOINT, "VASTDB_ENDPOINT should be defined");
        assertNotNull(VASTDB_AWS_ACCESS_KEY_ID, "VASTDB_AWS_ACCESS_KEY_ID should be defined");
        assertNotNull(VASTDB_AWS_SECRET_ACCESS_KEY, "VASTDB_AWS_SECRET_ACCESS_KEY should be defined");
        assertNotNull(VASTDB_KEYSPACE, "VASTDB_KEYSPACE should be defined");
        assertNotNull(VASTDB_BATCH_SIZE, "VASTDB_BATCH_SIZE should be defined");
        
        // Test default values
        assertEquals("janusgraph", VASTDB_KEYSPACE.getDefaultValue());
        assertEquals(Integer.valueOf(1000), VASTDB_BATCH_SIZE.getDefaultValue());
        assertEquals(Boolean.TRUE, VASTDB_AUTO_CREATE_TABLES.getDefaultValue());
        assertEquals("jg_", VASTDB_TABLE_PREFIX.getDefaultValue());
        
        log.info("Configuration options test passed");
    }
    
    /**
     * Create a test BaseTransactionConfig for testing.
     */
    private BaseTransactionConfig createTestTransactionConfig() {
        // Instead of implementing BaseTransactionConfig manually, let's try a different approach
        // Use the VastDBTransaction itself to create a proper config
        
        // Create a simple mock config that should work
        Configuration config = createTestConfiguration();
        
        // Create a minimal implementation that extends an existing class if possible
        // or use a different approach entirely
        return new TestTransactionConfig(config);
    }
    
    /**
     * Simple test implementation of BaseTransactionConfig
     */
    private static class TestTransactionConfig implements BaseTransactionConfig {
        private final Configuration config;
        private Instant commitTime = Instant.now();
        
        public TestTransactionConfig(Configuration config) {
            this.config = config;
        }
        
        @Override
        public Instant getCommitTime() {
            return commitTime;
        }
        
        @Override
        public void setCommitTime(Instant time) {
            this.commitTime = time;
        }
        
        @Override
        public boolean hasCommitTime() {
            return commitTime != null;
        }
        
        @Override
        public Configuration getCustomOptions() {
            return config;
        }
        
        @Override
        public <V> V getCustomOption(ConfigOption<V> option) {
            return config.get(option);
        }
        
        @Override
        public TimestampProvider getTimestampProvider() {
            return TimestampProviders.MICRO;
        }
        
        @Override
        public boolean hasGroupName() {
            return false;
        }
        
        @Override
        public String getGroupName() {
            return null;
        }
    }
    
    @Test
    public void testVastDBTransactionCreation() {
        // Test VastDBTransaction creation and basic lifecycle
        BaseTransactionConfig config = createTestTransactionConfig();
        VastDBTransaction transaction = new VastDBTransaction(config);
        
        assertTrue(transaction.isActive(), "New transaction should be active");
        assertFalse(transaction.isCommitted(), "New transaction should not be committed");
        assertFalse(transaction.isRolledBack(), "New transaction should not be rolled back");
        
        assertNotNull(transaction.getStartTime(), "Start time should be set");
        assertTrue(transaction.getDurationMillis() >= 0, "Duration should be non-negative");
        
        log.info("Transaction creation test passed");
    }
    
    @Test
    public void testVastDBTransactionCommit() {
        BaseTransactionConfig config = createTestTransactionConfig();
        VastDBTransaction transaction = new VastDBTransaction(config);
        
        assertTrue(transaction.isActive(), "New transaction should be active");
        
        // Test commit
        transaction.commit();
        assertFalse(transaction.isActive(), "Committed transaction should not be active");
        assertTrue(transaction.isCommitted(), "Transaction should be committed");
        assertFalse(transaction.isRolledBack(), "Committed transaction should not be rolled back");
        
        log.info("Transaction commit test passed");
    }
    
    @Test 
    public void testVastDBTransactionRollback() {
        BaseTransactionConfig config = createTestTransactionConfig();
        VastDBTransaction transaction = new VastDBTransaction(config);
        
        assertTrue(transaction.isActive(), "New transaction should be active");
        
        // Test rollback
        transaction.rollback();
        assertFalse(transaction.isActive(), "Rolled back transaction should not be active");
        assertFalse(transaction.isCommitted(), "Rolled back transaction should not be committed");
        assertTrue(transaction.isRolledBack(), "Transaction should be rolled back");
        
        log.info("Transaction rollback test passed");
    }
    
    @Test
    public void testStaticBufferOperations() {
        // Test basic StaticBuffer operations that our store will use
        StaticBuffer buffer1 = string2Buffer("test1");
        StaticBuffer buffer2 = string2Buffer("test2");
        StaticBuffer buffer3 = string2Buffer("test1");
        
        assertNotEquals(buffer1, buffer2, "Different buffers should not be equal");
        assertEquals(buffer1, buffer3, "Same content buffers should be equal");
        
        assertTrue(buffer1.compareTo(buffer2) < 0, "test1 should come before test2");
        assertEquals(0, buffer1.compareTo(buffer3), "Same content should have same comparison");
        
        // Test empty buffer
        StaticBuffer emptyBuffer = createEmptyBuffer();
        assertNotNull(emptyBuffer, "Empty buffer should not be null");
        assertEquals(0, emptyBuffer.length(), "Empty buffer should have zero length");
        
        log.info("StaticBuffer operations test passed");
    }
    
    @Test
    public void testEntryOperations() {
        StaticBuffer column = string2Buffer("column1");
        StaticBuffer value = string2Buffer("value1");
        
        Entry entry = StaticArrayEntry.of(column, value);

        assertEquals(column, entry.getColumn(), "Entry column should match");
        assertEquals(value, entry.getValue(), "Entry value should match");

        // Test entry with timestamp is not supported in this JanusGraph version; skipping.

        log.info("Entry operations test passed");
    }
    
    @Test 
    public void testSliceQueryCreation() {
        StaticBuffer start = string2Buffer("a");
        StaticBuffer end = string2Buffer("z");
        
        SliceQuery query = new SliceQuery(start, end);
        
        assertEquals(start, query.getSliceStart(), "Query start should match");
        assertEquals(end, query.getSliceEnd(), "Query end should match");
        
        // Test with limit
        SliceQuery queryWithLimit = new SliceQuery(start, end).setLimit(100);
        assertEquals(100, queryWithLimit.getLimit(), "Query limit should match");
        
        log.info("SliceQuery creation test passed");
    }
    
    @Test
    public void testKeySliceQueryCreation() {
        StaticBuffer key = string2Buffer("testkey");
        StaticBuffer start = string2Buffer("a");
        StaticBuffer end = string2Buffer("z");
        
        SliceQuery sliceQuery = new SliceQuery(start, end);
        KeySliceQuery keyQuery = new KeySliceQuery(key, sliceQuery);
        
        assertEquals(key, keyQuery.getKey(), "Key should match");
        // The getSliceQuery() method is not available in modern JanusGraph; assertion skipped.
        // assertEquals(sliceQuery, keyQuery.getSliceQuery(), "Slice query should match");
        
        log.info("KeySliceQuery creation test passed");
    }
    
    @Test
    public void testConfigurationCreation() {
        Configuration config = createTestConfiguration();
        
        assertNotNull(config, "Configuration should not be null");
        
        // Test that we can read configuration values
        assertEquals("test_keyspace", config.get(VASTDB_KEYSPACE), "Keyspace should match");
        assertEquals(Integer.valueOf(100), config.get(VASTDB_BATCH_SIZE), "Batch size should match");
        assertTrue(config.get(VASTDB_AUTO_CREATE_TABLES), "Auto create tables should be true");
        
        log.info("Configuration creation test passed");
    }
    
    @Test
    @Disabled("Requires VAST DB connection - enable for integration testing")
    public void testStoreManagerCreation() throws Exception {
        // This test is disabled by default as it requires a real VAST DB connection
        // Enable it when you have VAST DB credentials configured
        
        Configuration config = createTestConfiguration();
        
        // Only run this test if we have real credentials
        String endpoint = System.getenv("VASTDB_ENDPOINT");
        if (endpoint == null || endpoint.startsWith("http://localhost")) {
            log.info("Skipping store manager test - no real VAST DB credentials");
            return;
        }
        
        VastDBStoreManager storeManager = new VastDBStoreManager(config);
        assertNotNull(storeManager, "Store manager should be created");
        assertEquals("vastdb", storeManager.getName(), "Store manager name should be 'vastdb'");

        var features = storeManager.getFeatures();
        assertNotNull(features, "Store features should be available");

        log.info("Store manager creation test passed");
    }
    
    @Test
    public void testPreferredTimestamps() {
        // Test that preferred timestamps are defined
        assertNotNull(PREFERRED_TIMESTAMPS, "Preferred timestamps should be defined");
        assertTrue(PREFERRED_TIMESTAMPS.length > 0, "Should have at least one preferred timestamp");
        
        // Check that we have the expected timestamp units
        boolean hasMicros = false;
        boolean hasMillis = false;
        for (java.time.temporal.ChronoUnit unit : PREFERRED_TIMESTAMPS) {
            if (unit == java.time.temporal.ChronoUnit.MICROS) hasMicros = true;
            if (unit == java.time.temporal.ChronoUnit.MILLIS) hasMillis = true;
        }
        
        assertTrue(hasMicros || hasMillis, "Should support micros or millis timestamps");
        
        log.info("Preferred timestamps test passed");
    }
    
    /**
     * Create a test configuration for VAST DB.
     * Uses environment variables for connection details.
     */
    private Configuration createTestConfiguration() {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        
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
        
        return config;
    }
    
    /**
     * Convert a string to a StaticBuffer for testing.
     */
    private StaticBuffer string2Buffer(String s) {
        return new StaticArrayBuffer(s.getBytes(StandardCharsets.UTF_8));
    }
    
    /**
     * Create an empty StaticBuffer.
     */
    private StaticBuffer createEmptyBuffer() {
        return new StaticArrayBuffer(new byte[0]);
    }
}