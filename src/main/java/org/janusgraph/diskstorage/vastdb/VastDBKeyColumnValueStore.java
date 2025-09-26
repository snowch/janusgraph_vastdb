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
import com.vastdata.client.error.VastException;
import com.vastdata.vdb.sdk.NoExternalRowIdColumnException;
import com.vastdata.vdb.sdk.Table;
import com.vastdata.vdb.sdk.VastSdk;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.janusgraph.diskstorage.vastdb.VastDBConfigOptions.*;

public class VastDBKeyColumnValueStore implements KeyColumnValueStore {
    
    private static final Logger log = LoggerFactory.getLogger(VastDBKeyColumnValueStore.class);
    
    private final VastSdk vastSdk;
    private final String keyspace;
    private final String tableName;
    private final Configuration configuration;
    private final VastDBStoreManager storeManager;
    private final RootAllocator allocator;
    
    private Table table;
    private Schema tableSchema;
    private boolean isInitialized = false;
    
    // Cache for row IDs to avoid multiple lookups
    private final Map<StaticBuffer, Long> keyToRowIdCache = new ConcurrentHashMap<>();
    
    public VastDBKeyColumnValueStore(VastSdk vastSdk, String keyspace, String tableName, 
                                    Configuration configuration, VastDBStoreManager storeManager) 
                                    throws BackendException {
        this.vastSdk = Preconditions.checkNotNull(vastSdk);
        this.keyspace = Preconditions.checkNotNull(keyspace);
        this.tableName = Preconditions.checkNotNull(tableName);
        this.configuration = Preconditions.checkNotNull(configuration);
        this.storeManager = Preconditions.checkNotNull(storeManager);
        this.allocator = new RootAllocator();
        
        initialize();
    }
    
    private void initialize() throws BackendException {
        try {
            String tablePrefix = configuration.get(VASTDB_TABLE_PREFIX);
            String fullTableName = tablePrefix + tableName;
            
            this.table = vastSdk.getTable(keyspace, fullTableName);
            
            // Try to load existing schema
            try {
                table.loadSchema();
                this.tableSchema = table.getSchema();
                isInitialized = true;
                log.info("Loaded existing table schema for {}/{}", keyspace, fullTableName);
            } catch (NoExternalRowIdColumnException | RuntimeException e) {
                // Table doesn't exist or has wrong schema - create it
                if (configuration.get(VASTDB_AUTO_CREATE_TABLES)) {
                    createTable(fullTableName);
                } else {
                    throw new PermanentBackendException(
                        "Table does not exist and auto-create is disabled: " + fullTableName, e);
                }
            }
            
        } catch (Exception e) {
            throw new PermanentBackendException("Failed to initialize table: " + tableName, e);
        }
    }
    
    private void createTable(String fullTableName) throws BackendException {
        try {
            log.info("Creating new table: {}/{}", keyspace, fullTableName);
            
            // Define schema for JanusGraph KCV store:
            // vastdb_rowid (UInt64) - automatically added
            // key (Binary) - the row key
            // column (Binary) - the column name
            // value (Binary) - the column value
            // timestamp (UInt64) - modification timestamp
            
            List<Field> fields = List.of(
                new Field("key", FieldType.nullable(ArrowType.Binary.INSTANCE), null),
                new Field("column", FieldType.nullable(ArrowType.Binary.INSTANCE), null),
                new Field("value", FieldType.nullable(ArrowType.Binary.INSTANCE), null),
                new Field("timestamp", FieldType.nullable(new ArrowType.Int(64, false)), null)
            );
            
            this.tableSchema = new Schema(fields);
            
            // Create table using internal VastClient API (similar to example)
            // This would need to be implemented using the VastClient from the SDK
            createTableInVastDB(fullTableName, fields);
            
            // Now load the schema
            table.loadSchema();
            isInitialized = true;
            
        } catch (Exception e) {
            throw new PermanentBackendException("Failed to create table: " + fullTableName, e);
        }
    }
    
    private void createTableInVastDB(String fullTableName, List<Field> fields) throws VastException {
        // This would use the internal VastClient API similar to the example
        // to create a table with the vastdb_rowid column for External RowID with Internal allocation
        log.info("Creating table in VAST DB: {}", fullTableName);
        // Implementation would use VastClient.createTable() 
    }
    
    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        Preconditions.checkNotNull(query);
        ensureInitialized();
        
        try {
            StaticBuffer key = query.getKey();
            SliceQuery slice = query.getSliceQuery();
            
            // Find all rows for this key within the column range
            List<Entry> entries = new ArrayList<>();
            
            // In a real implementation, this would query VAST DB for all rows where:
            // key = query.getKey() AND column >= slice.getSliceStart() AND column <= slice.getSliceEnd()
            // For now, we'll do a simplified approach
            
            VectorSchemaRoot results = queryRowsForKey(key, slice);
            if (results != null) {
                entries.addAll(parseResultsToEntries(results));
                results.close();
            }
            
            return new EntryArrayList(entries);
            
        } catch (Exception e) {
            throw convertException(e);
        }
    }
    
    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, 
                                                StoreTransaction txh) throws BackendException {
        Preconditions.checkNotNull(keys);
        Preconditions.checkNotNull(query);
        ensureInitialized();
        
        Map<StaticBuffer, EntryList> result = new java.util.HashMap<>();
        
        for (StaticBuffer key : keys) {
            KeySliceQuery ksq = new KeySliceQuery(key, query);
            EntryList entries = getSlice(ksq, txh);
            result.put(key, entries);
        }
        
        return result;
    }
    
    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, 
                      StoreTransaction txh) throws BackendException {
        Preconditions.checkNotNull(key);
        ensureInitialized();
        
        try {
            long timestamp = System.currentTimeMillis();
            
            // Handle deletions first
            if (deletions != null && !deletions.isEmpty()) {
                performDeletions(key, deletions, timestamp);
            }
            
            // Handle additions
            if (additions != null && !additions.isEmpty()) {
                performAdditions(key, additions, timestamp);
            }
            
        } catch (Exception e) {
            throw convertException(e);
        }
    }
    
    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, 
                           StoreTransaction txh) throws BackendException {
        // VAST DB doesn't support distributed locking
        throw new UnsupportedOperationException("Locking is not supported by VAST DB backend");
    }
    
    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
        Preconditions.checkNotNull(query);
        ensureInitialized();
        
        try {
            // Return an iterator over all keys in the specified range
            return new VastDBKeyIterator(query, table, tableSchema, allocator);
            
        } catch (Exception e) {
            throw convertException(e);
        }
    }
    
    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        // Convert to a full range query
        KeyRangeQuery keyQuery = new KeyRangeQuery(
            StaticBuffer.EMPTY_BUFFER, 
            null, 
            query
        );
        return getKeys(keyQuery, txh);
    }
    
    @Override
    public String getName() {
        return tableName;
    }
    
    @Override
    public void close() throws BackendException {
        try {
            if (allocator != null) {
                allocator.close();
            }
            keyToRowIdCache.clear();
            log.info("Closed VAST DB store: {}", tableName);
        } catch (Exception e) {
            throw new PermanentBackendException("Failed to close store: " + tableName, e);
        }
    }
    
    private VectorSchemaRoot queryRowsForKey(StaticBuffer key, SliceQuery slice) throws BackendException {
        try {
            // This is a simplified implementation. In reality, we would need to:
            // 1. Query VAST DB for all rows where key matches
            // 2. Filter by column range if specified
            // 3. Apply limits and ordering
            
            // For now, return null to indicate no results
            // A full implementation would construct appropriate Arrow vectors
            // and query the VAST table
            
            return null;
            
        } catch (Exception e) {
            throw convertException(e);
        }
    }
    
    private List<Entry> parseResultsToEntries(VectorSchemaRoot results) {
        List<Entry> entries = new ArrayList<>();
        
        try {
            VarBinaryVector columnVector = (VarBinaryVector) results.getVector("column");
            VarBinaryVector valueVector = (VarBinaryVector) results.getVector("value");
            UInt8Vector timestampVector = (UInt8Vector) results.getVector("timestamp");
            
            for (int i = 0; i < results.getRowCount(); i++) {
                if (!columnVector.isNull(i) && !valueVector.isNull(i)) {
                    byte[] columnBytes = columnVector.get(i);
                    byte[] valueBytes = valueVector.get(i);
                    long timestamp = timestampVector.isNull(i) ? 0 : timestampVector.get(i);
                    
                    StaticBuffer column = new StaticArrayBuffer(columnBytes);
                    StaticBuffer value = new StaticArrayBuffer(valueBytes);
                    
                    entries.add(new Entry(column, value, timestamp));
                }
            }
        } catch (Exception e) {
            log.warn("Failed to parse results to entries", e);
        }
        
        return entries;
    }
    
    private void performDeletions(StaticBuffer key, List<StaticBuffer> deletions, long timestamp) 
            throws BackendException {
        try {
            // Find and delete rows matching key and columns
            for (StaticBuffer column : deletions) {
                // In a full implementation, this would:
                // 1. Query for rows where key=key AND column=column
                // 2. Delete those rows using table.delete()
                
                log.debug("Deleting column {} for key {}", column, key);
            }
            
        } catch (Exception e) {
            throw convertException(e);
        }
    }
    
    private void performAdditions(StaticBuffer key, List<Entry> additions, long timestamp) 
            throws BackendException {
        try {
            int batchSize = configuration.get(VASTDB_BATCH_SIZE);
            
            // Create Arrow vectors for the batch
            VarBinaryVector keyVector = new VarBinaryVector("key", allocator);
            VarBinaryVector columnVector = new VarBinaryVector("column", allocator);
            VarBinaryVector valueVector = new VarBinaryVector("value", allocator);
            UInt8Vector timestampVector = new UInt8Vector("timestamp", allocator);
            
            keyVector.allocateNew(additions.size());
            columnVector.allocateNew(additions.size());
            valueVector.allocateNew(additions.size());
            timestampVector.allocateNew(additions.size());
            
            // Populate vectors
            for (int i = 0; i < additions.size(); i++) {
                Entry entry = additions.get(i);
                
                keyVector.set(i, key.as(StaticBuffer.ARRAY_FACTORY));
                columnVector.set(i, entry.getColumn().as(StaticBuffer.ARRAY_FACTORY));
                valueVector.set(i, entry.getValue().as(StaticBuffer.ARRAY_FACTORY));
                timestampVector.set(i, entry.hasTimestamp() ? entry.getTimestamp() : timestamp);
            }
            
            keyVector.setValueCount(additions.size());
            columnVector.setValueCount(additions.size());
            valueVector.setValueCount(additions.size());
            timestampVector.setValueCount(additions.size());
            
            // Create record batch and insert
            List<FieldVector> fieldVectors = List.of(keyVector, columnVector, valueVector, timestampVector);
            VectorSchemaRoot recordBatch = new VectorSchemaRoot(tableSchema, fieldVectors, additions.size());
            
            VectorSchemaRoot insertedRowIds = table.put(recordBatch);
            
            // Clean up
            recordBatch.close();
            insertedRowIds.close();
            
        } catch (Exception e) {
            throw convertException(e);
        }
    }
    
    private void ensureInitialized() throws BackendException {
        if (!isInitialized) {
            throw new PermanentBackendException("Store not properly initialized: " + tableName);
        }
    }
    
    private BackendException convertException(Exception e) {
        if (e instanceof VastException) {
            return new TemporaryBackendException("VAST DB operation failed", e);
        } else if (e instanceof BackendException) {
            return (BackendException) e;
        } else {
            return new PermanentBackendException("Unexpected error", e);
        }
    }
}
    