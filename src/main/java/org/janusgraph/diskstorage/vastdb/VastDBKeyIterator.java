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
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Iterator implementation for scanning keys in VAST DB tables.
 * 
 * Since VAST DB doesn't support SQL-like queries, this iterator works
 * with an in-memory index for efficient key scanning.
 */
public class VastDBKeyIterator implements KeyIterator {
    
    private static final Logger log = LoggerFactory.getLogger(VastDBKeyIterator.class);
    
    private final KeyRangeQuery query;
    private final Map<StaticBuffer, Map<StaticBuffer, Entry>> memoryIndex;
    
    private final Iterator<StaticBuffer> keyIterator;
    private StaticBuffer currentKey;
    private boolean closed = false;
    
    public VastDBKeyIterator(List<StaticBuffer> keys, KeyRangeQuery query, 
                            Map<StaticBuffer, Map<StaticBuffer, Entry>> memoryIndex) 
            throws BackendException {
        this.query = query;
        this.memoryIndex = memoryIndex;
        this.keyIterator = keys.iterator();
        
        log.debug("Created key iterator for {} keys", keys.size());
    }
    
    @Override
    public boolean hasNext() {
        if (closed) {
            return false;
        }
        
        return keyIterator.hasNext();
    }
    
    @Override
    public StaticBuffer next() {
        if (!hasNext()) {
            throw new java.util.NoSuchElementException("No more keys available");
        }
        
        currentKey = keyIterator.next();
        return currentKey;
    }
    
    @Override
    public RecordIterator<Entry> getEntries() {
        if (currentKey == null) {
            throw new IllegalStateException("Must call next() before getEntries()");
        }
        
        try {
            // Get all entries for the current key within the slice query constraints
            List<Entry> entries = loadEntriesForKey(currentKey);
            return new VastDBEntryIterator(entries);
            
        } catch (Exception e) {
            log.error("Failed to get entries for key: " + currentKey, e);
            return new VastDBEntryIterator(new ArrayList<>());
        }
    }
    
    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            log.debug("Closed VAST DB key iterator");
        }
    }
    
    /**
     * Load all entries for a specific key that match the slice query.
     */
    private List<Entry> loadEntriesForKey(StaticBuffer key) {
        List<Entry> entries = new ArrayList<>();
        
        Map<StaticBuffer, Entry> keyEntries = memoryIndex.get(key);
        if (keyEntries == null) {
            return entries;
        }
        
        SliceQuery slice = query.getSliceQuery();
        
        // Filter entries by slice constraints
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
        
        return entries;
    }
    
    /**
     * Check if a column is within the slice range.
     */
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
    
    /**
     * Simple iterator implementation for entries.
     */
    private static class VastDBEntryIterator implements RecordIterator<Entry> {
        private final Iterator<Entry> iterator;
        
        public VastDBEntryIterator(List<Entry> entries) {
            this.iterator = entries.iterator();
        }
        
        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }
        
        @Override
        public Entry next() {
            return iterator.next();
        }
        
        @Override
        public void close() {
            // Nothing to clean up
        }
    }
}