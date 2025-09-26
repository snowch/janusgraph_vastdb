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
import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import com.vastdata.client.schema.ArrowSchemaUtils;
import com.vastdata.client.schema.CreateTableContext;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.tx.SimpleVastTransaction;
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
import java.util.Collections;
import java.util.HashMap;
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
    
    // In-memory storage for the KCV data since VAST DB doesn't support SQL queries
    // In a production implementation, this would need to be replaced with a proper
    // indexing and scanning mechanism
    private final Map<StaticBuffer, Map<StaticBuffer, Entry>> memoryIndex = new ConcurrentHashMap<>();
    private long nextRowId = 0;
    
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
                
                // TODO: Load existing data into memory index if needed
                loadExistingData();
                
            } catch (NoExternalRowIdColumnException | RuntimeException e) {
                // Table doesn't exist - create it
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
            // vastdb_rowid (UInt64) - automatically added by VAST
            // key (Binary) - the row key
            // column (Binary) - the column name  
            // value (Binary) - the column value
            // timestamp (UInt64) - modification timestamp
            
            List<Field> fields = List.of(
                ArrowSchemaUtils.VASTDB_ROW_ID_FIELD, // Required for External RowID
                new Field("key", FieldType.nullable(ArrowType.Binary.INSTANCE), null),
                new Field("column", FieldType.nullable(ArrowType.Binary.INSTANCE), null), 
                new Field("value", FieldType.nullable(ArrowType.Binary.INSTANCE), null),
                new Field("timestamp", FieldType.nullable(new ArrowType.Int(64, false)), null)
            );
            
            // Use internal VAST client to create table
            VastClient client = vastSdk.getVastClient();
            VastDBTransactionManager txManager = storeManager.getTransactionManager();
            
            SimpleVastTransaction tx = txManager.startTransaction(
                new StartTransactionContext(false, false));
            
            client.createTable(tx, new CreateTableContext(keyspace, fullTableName, fields, null, null));
            txManager.commit(tx);
            
            // Now load the schema (excluding vastdb_rowid for our operations)
            table.loadSchema();
            this.tableSchema = table.getSchema();
            isInitialized = true;
            
            log.info("Successfully created table: {}/{}", keyspace, fullTableName);
            
        } catch (Exception e) {
            throw new PermanentBackendException("Failed to create table: " + fullTableName, e);
        }
    }
    
    private void loadExistingData() throws BackendException {
        // Since VAST DB doesn't support full table scans via SQL,
        // we can't easily load existing data into our memory index.
        // In a production implementation, you would need to:
        // 1. Maintain a separate index table/structure
        // 2. Use external indexing system  
        // 3. Or implement a scanning mechanism using the VAST SDK
        
        log.warn("Loading existing data not fully implemented - starting with empty index");
        // For now, start with empty memory index
    }
    
    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        Preconditions.checkNotNull(query);
        ensureInitialized();
        
        try {
            StaticBuffer key = query.getKey();
            SliceQuery slice = query.getSliceQuery();
            
            // Get entries from memory index
            Map<StaticBuffer, Entry> keyEntries = memoryIndex.get(key);
            if (keyEntries == null) {
                return new EntryArrayList();
            }
            
            List<Entry> entries = new ArrayList<>();
            
            // Filter by slice range
            for (Map.Entry<StaticBuffer, Entry> entry : keyEntries.entrySet()) {
                StaticBuffer column = entry.getKey();
                Entry value = entry.getValue();
                
                // Check if column is within slice range
                if (isColumnInSlice(column, slice)) {
                    entries.add(value);
                }
            }
            
            // Sort entries by column
            entries.sort((e1, e2) -> e1.getColumn().compareTo(e2.getColumn()));
            
            // Apply limit if specified
            if (slice.hasLimit()) {
                int limit = slice.getLimit();
                if (entries.size() > limit) {
                    entries = entries.subList(0, limit);
                }
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
        
        Map<StaticBuffer, EntryList> result = new HashMap<>();
        
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
            
            // Update memory index first
            Map<StaticBuffer, Entry> keyEntries = memoryIndex.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
            
            // Handle deletions
            if (deletions != null && !deletions.isEmpty()) {
                for (StaticBuffer column : deletions) {
                    keyEntries.remove(column);
                }
                performDeletionsInVastDB(key, deletions, timestamp);
            }
            
            // Handle additions
            if (additions != null && !additions.isEmpty()) {
                for (Entry entry : additions) {
                    keyEntries.put(entry.getColumn(), entry);
                }
                performAdditionsInVastDB(key, additions, timestamp);
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
            // Return an iterator over keys from memory index
            List<StaticBuffer> keys = new ArrayList<>();
            
            for (StaticBuffer key : memoryIndex.keySet()) {
                if (isKeyInRange(key, query)) {
                    keys.add(key);
                }
            }
            
            keys.sort(StaticBuffer::compareTo);
            
            return new VastDBKeyIterator(keys, query, memoryIndex);
            
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
            memoryIndex.clear();
            log.info("Closed VAST DB store: {}", tableName);
        } catch (Exception e) {
            throw new PermanentBackendException("Failed to close store: " + tableName, e);
        }
    }
    
    private boolean isColumnInSlice(StaticBuffer column, SliceQuery slice) {
        StaticBuffer sliceStart = slice.getSliceStart();
        StaticBuffer sliceEnd = slice.getSliceEnd();
        
        if (sliceStart != null && sliceStart.length() > 0 && column.compareTo(sliceStart) < 0) {
            return false;
        }
        
        if (sliceEnd != null && sliceEnd.length() > 0 && column.compareTo(sliceEnd) >= 0) {
            return false;
        }
        
        return true;
    }
    
    private boolean isKeyInRange(StaticBuffer key, KeyRangeQuery query) {
        StaticBuffer keyStart = query.getKeyStart();
        StaticBuffer keyEnd = query.getKeyEnd();
        
        if (keyStart != null && key.compareTo(keyStart) < 0) {
            return false;
        }
        
        if (keyEnd != null && key.compareTo(keyEnd) >= 0) {
            return false;
        }
        
        return true;
    }
    
    private void performDeletionsInVastDB(StaticBuffer key, List<StaticBuffer> deletions, long timestamp) 
            throws BackendException {
        // Since VAST DB doesn't support SQL DELETE, we would need to:
        // 1. Find the row IDs of rows to delete (requires maintaining a row ID index)
        // 2. Use table.delete() with those row IDs
        
        // For now, log the operation
        log.debug("Performing deletions in VAST DB for key {} (not fully implemented)", key);
        
        // TODO: Implement proper deletion using row IDs
    }
    
    private void performAdditionsInVastDB(StaticBuffer key, List<Entry> additions, long timestamp) 
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
            
            // TODO: Store row IDs for future deletion support
            processInsertedRowIds(key, additions, insertedRowIds);
            
            // Clean up
            recordBatch.close();
            insertedRowIds.close();
            
        } catch (Exception e) {
            throw convertException(e);
        }
    }
    
    private void processInsertedRowIds(StaticBuffer key, List<Entry> additions, VectorSchemaRoot rowIds) {
        // Store the mapping of key+column -> rowId for future deletion support
        // This would require a separate index structure in a production implementation
        log.debug("Processed {} inserted row IDs for key {}", rowIds.getRowCount(), key);
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