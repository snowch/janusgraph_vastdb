# JanusGraph VAST Database Backend

This module provides a JanusGraph storage backend implementation for [VAST Database](https://www.vastdata.com/), enabling JanusGraph to use VAST's high-performance analytics platform as its storage layer.

## Features

- **High Performance**: Leverages VAST Database's columnar storage and vectorized processing for fast graph operations
- **Scalability**: Built on VAST's distributed architecture for handling large-scale graph data
- **Apache Arrow Integration**: Uses Arrow format for efficient data transfer between JanusGraph and VAST
- **Transactional Support**: Provides JanusGraph transaction semantics with appropriate consistency guarantees
- **Key-Value Abstraction**: Maps JanusGraph's key-column-value model to VAST's table structure

## Requirements

- JanusGraph 1.2.0+
- VAST Database 5.3.0+
- Java 17+
- Apache Arrow 13.0.0+

## Configuration

### Basic Configuration

```properties
# Storage backend
storage.backend=vastdb

# VAST Database connection
storage.vastdb.endpoint=https://your-vast-cluster.com
storage.vastdb.aws-access-key-id=your-access-key
storage.vastdb.aws-secret-access-key=your-secret-key
storage.vastdb.keyspace=janusgraph

# Optional performance tuning
storage.vastdb.batch-size=1000
storage.vastdb.connection-timeout-ms=30000
storage.vastdb.read-timeout-ms=60000
```

### Environment Variables

You can also configure the backend using environment variables:

```bash
export VASTDB_ENDPOINT=https://your-vast-cluster.com
export VASTDB_AWS_ACCESS_KEY_ID=your-access-key
export VASTDB_AWS_SECRET_ACCESS_KEY=your-secret-key
```

### Configuration Options

| Property | Description | Default |
|----------|-------------|---------|
| `storage.vastdb.endpoint` | VAST Database cluster endpoint URI | Required |
| `storage.vastdb.aws-access-key-id` | AWS access key ID for authentication | Required |
| `storage.vastdb.aws-secret-access-key` | AWS secret access key for authentication | Required |
| `storage.vastdb.keyspace` | VAST Database keyspace/schema name | `janusgraph` |
| `storage.vastdb.auto-create-tables` | Automatically create tables when they don't exist | `true` |
| `storage.vastdb.table-prefix` | Prefix to add to all table names | `jg_` |
| `storage.vastdb.batch-size` | Batch size for bulk operations | `1000` |
| `storage.vastdb.connection-timeout-ms` | Connection timeout in milliseconds | `30000` |
| `storage.vastdb.read-timeout-ms` | Read timeout in milliseconds | `60000` |
| `storage.vastdb.max-retry-attempts` | Maximum retry attempts for failed operations | `3` |
| `storage.vastdb.retry-delay-ms` | Initial delay between retry attempts | `100` |

## Architecture

### Data Model

JanusGraph's key-column-value model is mapped to VAST Database tables as follows:

- **Key**: Row identifier (stored in `key` column)
- **Column**: Column name within the row (stored in `column` column) 
- **Value**: Column value (stored in `value` column)
- **Timestamp**: Modification timestamp (stored in `timestamp` column)
- **Row ID**: VAST's internal row identifier (`vastdb_rowid`)

Each JanusGraph "store" (vertex store, edge store, index stores, etc.) becomes a separate VAST table.

### Table Schema

```sql
CREATE TABLE keyspace.jg_store_name (
    vastdb_rowid UInt64,    -- VAST internal row ID
    key          Binary,     -- JanusGraph key
    column       Binary,     -- JanusGraph column  
    value        Binary,     -- JanusGraph value
    timestamp    UInt64      -- Modification timestamp
);
```

### Transaction Handling

VAST Database doesn't support distributed ACID transactions, so this backend provides:

- **Atomicity**: Individual operations are atomic
- **Consistency**: Data consistency maintained through careful operation ordering
- **Isolation**: Read-committed isolation level
- **Durability**: All operations are immediately durable

## Usage Examples

### Programmatic Configuration

```java
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import static org.janusgraph.diskstorage.vastdb.VastDBConfigOptions.*;

ModifiableConfiguration config = new ModifiableConfiguration();
config.set(STORAGE_BACKEND, "vastdb");
config.set(VASTDB_ENDPOINT, "https://your-vast-cluster.com");
config.set(VASTDB_AWS_ACCESS_KEY_ID, "your-access-key");  
config.set(VASTDB_AWS_SECRET_ACCESS_KEY, "your-secret-key");
config.set(VASTDB_KEYSPACE, "mygraph");

JanusGraph graph = JanusGraphFactory.open(config);
```

### Properties File Configuration

Create a `janusgraph-vastdb.properties` file:

```properties
storage.backend=vastdb
storage.vastdb.endpoint=https://your-vast-cluster.com
storage.vastdb.aws-access-key-id=your-access-key
storage.vastdb.aws-secret-access-key=your-secret-key
storage.vastdb.keyspace=mygraph
storage.vastdb.batch-size=2000

# Optional: Configure index backend
index.search.backend=elasticsearch
index.search.hostname=your-es-cluster
```

Then open the graph:

```java
JanusGraph graph = JanusGraphFactory.open("janusgraph-vastdb.properties");
```

## Performance Considerations

### Batch Operations

- Configure appropriate `batch-size` for your workload
- Larger batches improve throughput but use more memory
- Monitor VAST cluster performance and adjust accordingly

### Indexing Strategy

- Use external index backends (Elasticsearch, Solr) for complex queries
- VAST provides efficient range scans for simple key-based access
- Consider VAST's built-in indexing capabilities for performance

### Schema Design

- Design your graph schema to leverage VAST's columnar storage
- Group related properties to minimize cross-table joins
- Use appropriate data types to optimize storage and query performance

## Limitations

### Current Limitations

- **Locking**: No distributed locking support (not required for most graph operations)
- **TTL**: No native cell-level time-to-live support
- **Transactions**: Limited transaction semantics (immediate durability, no rollback)
- **Schema Evolution**: Manual schema changes may be required for major upgrades

### VAST Database Requirements  

- Tables must have **External RowID** enabled with **Internal RowID Allocation**
- First insertion determines allocation strategy (don't include `vastdb_rowid` in first batch)
- Only `get`, `put`, and `delete` operations supported (no `update`)

## Testing

### Unit Tests

Run the test suite:

```bash
mvn test
```

### Integration Tests

For integration testing, ensure you have:

1. Access to a VAST Database cluster
2. Valid AWS credentials configured
3. Environment variables set:

```bash
export VASTDB_ENDPOINT=https://your-vast-cluster.com
export VASTDB_AWS_ACCESS_KEY_ID=your-access-key
export VASTDB_AWS_SECRET_ACCESS_KEY=your-secret-key
```

Run integration tests:

```bash
mvn verify -Pintegration-tests
```

## Building

### Prerequisites

- Maven 3.6+
- Java 17+
- Access to VAST Database Maven repository

### Build Commands

```bash
# Clean build
mvn clean compile

# Run tests
mvn test

# Create distribution
mvn package

# Install to local repository  
mvn install
```

### Maven Repository

Add the VAST Database Maven repository to your `pom.xml`:

```xml
<repositories>
    <repository>
        <id>vastdb-maven-release</id>
        <url>https://vast-maven-repo.s3.amazonaws.com/release</url>
    </repository>
</repositories>
```

## Troubleshooting

### Common Issues

1. **Connection Timeout**: Increase `connection-timeout-ms` and `read-timeout-ms`
2. **Authentication Errors**: Verify AWS credentials have proper VAST Database permissions
3. **Schema Errors**: Ensure tables have correct External RowID configuration
4. **Performance Issues**: Adjust batch size and monitor VAST cluster metrics

### Logging

Enable debug logging for the VAST backend:

```xml
<logger name="org.janusgraph.diskstorage.vastdb" level="DEBUG"/>
<logger name="com.vastdata.vdb.sdk" level="DEBUG"/>
```

### Monitoring

Monitor key metrics:
- Query latency and throughput
- VAST cluster CPU and memory usage  
- Network I/O between JanusGraph and VAST
- Error rates and retry attempts

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

For issues and questions:

- JanusGraph: [JanusGraph GitHub Issues](https://github.com/JanusGraph/janusgraph/issues)
- VAST Database: [VAST Support Portal](https://support.vastdata.com/)

## See Also

- [JanusGraph Documentation](https://docs.janusgraph.org/)
- [VAST Database Documentation](https://support.vastdata.com/)
- [Apache Arrow Documentation](https://arrow.apache.org/docs/)
