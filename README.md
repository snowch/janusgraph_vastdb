# JanusGraph VAST Database Backend

This module provides a JanusGraph storage backend implementation for [VAST Database](https://www.vastdata.com/), enabling JanusGraph to use VAST's high-performance analytics platform as its storage layer.

## Features

- **High Performance**: Leverages VAST Database's columnar storage and vectorized processing for fast graph operations
- **Scalability**: Built on VAST's distributed architecture for handling large-scale graph data
- **Apache Arrow Integration**: Uses Arrow format for efficient data transfer between JanusGraph and VAST
- **Transactional Support**: Provides JanusGraph transaction semantics with appropriate consistency guarantees
- **Key-Value Abstraction**: Maps JanusGraph's key-column-value model to VAST's table structure

## Requirements

- JanusGraph 1.0.0+
- VAST Database 5.3.0+
- Java 17+
- Apache Arrow 13.0.0+

## Architecture Limitations & Design

### Important Note: No SQL Support

**VAST Database SDK does not support SQL queries.** This backend implementation uses a hybrid approach:

1. **Data Storage**: All data is stored in VAST DB tables using Apache Arrow format
2. **Query Processing**: An in-memory index is maintained for query operations since VAST DB doesn't support SQL SELECT/WHERE operations
3. **Limitations**: This approach has scalability limitations and is suitable for development/testing rather than large-scale production use

### Recommended Production Approach

For production deployments, consider:

1. **External Indexing**: Use a separate indexing system (Redis, Elasticsearch) alongside VAST DB storage
2. **Custom Query Engine**: Implement a query engine that can efficiently scan VAST DB tables
3. **Hybrid Storage**: Use VAST DB for bulk storage with a faster database for indexes

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

## Data Model

### Table Schema

JanusGraph's key-column-value model is mapped to VAST Database tables as follows:

```sql
CREATE TABLE keyspace.jg_store_name (
    vastdb_rowid UInt64,    -- VAST internal row ID (required for External RowID)
    key          Binary,     -- JanusGraph key
    column       Binary,     -- JanusGraph column  
    value        Binary,     -- JanusGraph value
    timestamp    UInt64      -- Modification timestamp
);
```

### Query Processing

Since VAST DB doesn't support SQL queries:

1. **Writes**: Data is written directly to VAST DB tables using Arrow format
2. **Reads**: An in-memory index maps keys to their column-value pairs
3. **Scans**: Key iteration uses the in-memory index for filtering and range queries

### Transaction Handling

VAST Database doesn't support distributed ACID transactions, so this backend provides:

- **Atomicity**: Individual Arrow batch operations are atomic
- **Consistency**: Data consistency maintained through careful operation ordering
- **Isolation**: Read-committed isolation level via in-memory index
- **Durability**: All operations are immediately durable in VAST DB

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

## Limitations

### Current Implementation Limitations

- **Memory Constraints**: In-memory indexing limits scalability
- **Query Performance**: No native query optimization from VAST DB
- **Delete Operations**: Row-level deletes not fully implemented (requires row ID tracking)
- **Data Loading**: Cannot efficiently load existing data from VAST DB tables
- **Locking**: No distributed locking support
- **TTL**: No native cell-level time-to-live support

### VAST Database Requirements  

- Tables must have **External RowID** enabled with **Internal RowID Allocation**
- First insertion determines allocation strategy (don't include `vastdb_rowid` in first batch)
- Only `get`, `put`, and `delete` operations supported via SDK
- No SQL query support

### Scalability Concerns

This implementation is **not recommended for production use** with large datasets due to:

1. **Memory Usage**: All keys maintained in memory
2. **Startup Time**: No mechanism to reload existing data
3. **Query Limitations**: Cannot leverage VAST DB's columnar query capabilities

## Performance Considerations

### Batch Operations

- Configure appropriate `batch-size` for your workload
- Larger batches improve throughput but use more memory
- Monitor VAST cluster performance and adjust accordingly

### Memory Management

- Monitor JVM heap usage as graph size grows
- Consider implementing LRU cache for memory index
- Use appropriate JVM memory settings

### Schema Design

- Design your graph schema to minimize key diversity
- Group related properties to reduce memory overhead
- Consider using shorter key representations

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

1. **Memory Issues**: Increase JVM heap size with `-Xmx` settings
2. **Connection Timeout**: Increase `connection-timeout-ms` and `read-timeout-ms`
3. **Authentication Errors**: Verify AWS credentials have proper VAST Database permissions
4. **Schema Errors**: Ensure tables have correct External RowID configuration
5. **Performance Issues**: Monitor memory usage and consider data partitioning

### Logging

Enable debug logging for the VAST backend:

```xml
<logger name="org.janusgraph.diskstorage.vastdb" level="DEBUG"/>
<logger name="com.vastdata.vdb.sdk" level="DEBUG"/>
```

### Monitoring

Monitor key metrics:
- JVM heap memory usage
- In-memory index size
- VAST cluster CPU and memory usage  
- Network I/O between JanusGraph and VAST
- Error rates and retry attempts

## Future Improvements

### Possible Enhancements

1. **Persistent Indexing**: Implement persistent index storage in VAST DB
2. **Lazy Loading**: Load index data on-demand rather than keeping everything in memory
3. **Query Optimization**: Develop custom query engine for VAST DB scanning
4. **Compression**: Implement key compression for memory efficiency
5. **Sharding**: Support horizontal partitioning across multiple VAST tables

### Production Readiness

To make this backend production-ready:

1. Replace in-memory indexing with persistent solution
2. Implement efficient data loading mechanisms
3. Add comprehensive monitoring and metrics
4. Optimize for VAST DB's columnar storage characteristics
5. Implement proper row-level delete functionality

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
- [VAST Database Java SDK Example](./java-sdk-example/README.md)