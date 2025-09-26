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

import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;

import java.time.temporal.ChronoUnit;

@PreInitializeConfigOptions
public interface VastDBConfigOptions {
    
    ConfigNamespace VASTDB_NS = new ConfigNamespace(
        GraphDatabaseConfiguration.STORAGE_NS, 
        "vastdb", 
        "VAST Database storage backend options"
    );
    
    // Connection configuration
    ConfigOption<String> VASTDB_ENDPOINT = new ConfigOption<>(
        VASTDB_NS,
        "endpoint",
        "VAST Database cluster endpoint URI",
        ConfigOption.Type.LOCAL,
        String.class
    );
    
    ConfigOption<String> VASTDB_AWS_ACCESS_KEY_ID = new ConfigOption<>(
        VASTDB_NS,
        "aws-access-key-id",
        "AWS access key ID for VAST Database authentication",
        ConfigOption.Type.LOCAL,
        String.class
    );
    
    ConfigOption<String> VASTDB_AWS_SECRET_ACCESS_KEY = new ConfigOption<>(
        VASTDB_NS,
        "aws-secret-access-key",
        "AWS secret access key for VAST Database authentication",
        ConfigOption.Type.LOCAL,
        String.class
    );
    
    ConfigOption<String> VASTDB_KEYSPACE = new ConfigOption<>(
        VASTDB_NS,
        "keyspace",
        "VAST Database keyspace/schema name",
        ConfigOption.Type.LOCAL,
        "janusgraph"
    );
    
    // Performance and behavior configuration
    ConfigOption<Integer> VASTDB_BATCH_SIZE = new ConfigOption<>(
        VASTDB_NS,
        "batch-size",
        "Batch size for bulk operations",
        ConfigOption.Type.MASKABLE,
        1000
    );
    
    ConfigOption<Boolean> VASTDB_ENABLE_OPTIMISTIC_LOCKING = new ConfigOption<>(
        VASTDB_NS,
        "enable-optimistic-locking",
        "Enable optimistic locking for transactions",
        ConfigOption.Type.MASKABLE,
        false
    );
    
    
    ConfigOption<Integer> VASTDB_CONNECTION_TIMEOUT_MS = new ConfigOption<>(
        VASTDB_NS,
        "connection-timeout-ms",
        "Connection timeout in milliseconds",
        ConfigOption.Type.LOCAL,
        30000
    );
    
    ConfigOption<Integer> VASTDB_READ_TIMEOUT_MS = new ConfigOption<>(
        VASTDB_NS,
        "read-timeout-ms", 
        "Read timeout in milliseconds",
        ConfigOption.Type.LOCAL,
        60000
    );
    
    // Table configuration
    ConfigOption<Boolean> VASTDB_AUTO_CREATE_TABLES = new ConfigOption<>(
        VASTDB_NS,
        "auto-create-tables",
        "Automatically create tables when they don't exist",
        ConfigOption.Type.LOCAL,
        true
    );
    
    ConfigOption<String> VASTDB_TABLE_PREFIX = new ConfigOption<>(
        VASTDB_NS,
        "table-prefix",
        "Prefix to add to all table names",
        ConfigOption.Type.LOCAL,
        "jg_"
    );
    
    // Retry configuration
    ConfigOption<Integer> VASTDB_MAX_RETRY_ATTEMPTS = new ConfigOption<>(
        VASTDB_NS,
        "max-retry-attempts",
        "Maximum number of retry attempts for failed operations",
        ConfigOption.Type.MASKABLE,
        3
    );
    
    ConfigOption<Integer> VASTDB_RETRY_DELAY_MS = new ConfigOption<>(
        VASTDB_NS,
        "retry-delay-ms",
        "Initial delay between retry attempts in milliseconds",
        ConfigOption.Type.MASKABLE,
        100
    );
    
    // Buffer sizes
    ConfigOption<Integer> VASTDB_BUFFER_SIZE = new ConfigOption<>(
        VASTDB_NS,
        "buffer-size",
        "Size of internal buffers in bytes",
        ConfigOption.Type.LOCAL,
        1024 * 1024 // 1MB
    );
    
    // Preferred timestamps setting
    ChronoUnit[] PREFERRED_TIMESTAMPS = {ChronoUnit.MICROS, ChronoUnit.MILLIS};
}
