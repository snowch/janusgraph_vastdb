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

import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.tx.ParsedStartTransactionResponse;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTransactionHandleManager;
import com.vastdata.client.tx.VastTransactionInstantiator;

/**
 * Transaction manager for VAST DB operations.
 * 
 * This class manages VAST DB transactions for schema operations like
 * creating/dropping tables and schemas.
 */
public class VastDBTransactionManager extends VastTransactionHandleManager<SimpleVastTransaction> {
    
    private static final SimpleVastTransactionFactory TRANSACTION_FACTORY = new SimpleVastTransactionFactory();
    
    public VastDBTransactionManager(VastClient client) {
        super(client, TRANSACTION_FACTORY);
    }
    
    /**
     * Factory for creating SimpleVastTransaction instances.
     */
    private static class SimpleVastTransactionFactory 
            implements VastTransactionInstantiator<SimpleVastTransaction> {
        
        @Override
        public SimpleVastTransaction apply(StartTransactionContext startTransactionContext, 
                                         ParsedStartTransactionResponse parsedStartTransactionResponse) {
            return new SimpleVastTransaction(
                parsedStartTransactionResponse.getId(),
                startTransactionContext.isReadOnly(),
                startTransactionContext.isAutoCommit()
            );
        }
    }
}