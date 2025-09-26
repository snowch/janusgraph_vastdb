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

import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Transaction implementation for VAST DB backend.
 * 
 * VAST DB doesn't natively support distributed transactions, so this implementation
 * focuses on providing transaction semantics at the JanusGraph level while
 * ensuring consistency through careful operation ordering and error handling.
 */
public class VastDBTransaction extends AbstractStoreTransaction {
    
    private static final Logger log = LoggerFactory.getLogger(VastDBTransaction.class);
    
    private final Instant startTime;
    private boolean isCommitted = false;
    private boolean isRolledBack = false;
    
    public VastDBTransaction(BaseTransactionConfig config) {
        super(config);
        this.startTime = Instant.now();
        
        log.debug("Started VAST DB transaction with configuration: {}", config);
    }
    
    /**
     * Commit the transaction.
     * 
     * Since VAST DB doesn't support distributed transactions, this mainly serves
     * to mark the transaction as completed and perform any necessary cleanup.
     */
    @Override
    public void commit() {
        if (isCommitted || isRolledBack) {
            log.warn("Attempting to commit transaction that is already completed");
            return;
        }
        
        try {
            // Perform any necessary cleanup or final operations
            log.debug("Committing VAST DB transaction");
            
            isCommitted = true;
            
        } catch (Exception e) {
            log.error("Failed to commit VAST DB transaction", e);
            throw new RuntimeException("Transaction commit failed", e);
        }
    }
    
    /**
     * Rollback the transaction.
     * 
     * Since VAST DB operations are immediately committed, rollback support
     * is limited. This mainly serves to mark the transaction as rolled back
     * and prevent further operations.
     */
    @Override
    public void rollback() {
        if (isCommitted || isRolledBack) {
            log.warn("Attempting to rollback transaction that is already completed");
            return;
        }
        
        try {
            log.debug("Rolling back VAST DB transaction");
            
            // Note: True rollback is not possible with VAST DB since operations
            // are immediately persisted. Applications should handle compensation
            // logic at a higher level if needed.
            
            isRolledBack = true;
            
        } catch (Exception e) {
            log.error("Failed to rollback VAST DB transaction", e);
            throw new RuntimeException("Transaction rollback failed", e);
        }
    }
    
    /**
     * Check if the transaction has been committed.
     */
    public boolean isCommitted() {
        return isCommitted;
    }
    
    /**
     * Check if the transaction has been rolled back.
     */
    public boolean isRolledBack() {
        return isRolledBack;
    }
    
    /**
     * Check if the transaction is still active (not committed or rolled back).
     */
    public boolean isActive() {
        return !isCommitted && !isRolledBack;
    }
    
    /**
     * Get the transaction start time.
     */
    public Instant getStartTime() {
        return startTime;
    }
    
    /**
     * Get the transaction duration up to now or completion.
     */
    public long getDurationMillis() {
        return java.time.Duration.between(startTime, Instant.now()).toMillis();
    }
    
    @Override
    public String toString() {
        return String.format("VastDBTransaction{startTime=%s, committed=%s, rolledBack=%s, duration=%dms}",
            startTime, isCommitted, isRolledBack, getDurationMillis());
    }
}