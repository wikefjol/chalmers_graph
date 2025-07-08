# Graph RAG Neo4j Implementation

A protocol-based implementation for extracting research data from Elasticsearch and building a Graph RAG system with Neo4j. Features streaming architecture for handling large datasets (200K+ records) without memory issues.

## Project Overview

This project implements a type-safe, protocol-based architecture for:
1. **Streaming Data Extraction**: Pull research data from Elasticsearch 6.8.23 using scroll API
2. **Batch Processing**: Process data in configurable batches to handle large datasets
3. **Graph Database**: Load into Neo4j for Graph RAG queries with progress tracking
4. **Embedding Integration**: Support for OpenAI embeddings and vector search (planned)

## Current Status (January 2025)

### ✅ Working
- Elasticsearch integration with scroll API
- Streaming extractors for all entity types
- Neo4j schema management and constraints
- CLI interface with comprehensive commands
- Document formatting evaluation system
- Full dataset imports with relationship processing
- Memory-efficient streaming architecture

### ❌ Not Implemented
- Embedding generation pipeline
- Vector search functionality
- RAG query processing
- Checkpoint/recovery system

## Key Features

- **Memory-Efficient Streaming**: Process millions of records without loading all into memory
- **Configurable Batch Sizes**: Adjust based on available memory and dataset size
- **Progress Tracking**: Real-time updates during import operations
- **Dual Pipeline Support**: Legacy pipeline for small datasets, streaming for large ones
- **Backward Compatible**: Existing code continues to work with new streaming extractors

## Architecture

### Protocol-Based Design
We use Python protocols (PEP 544) to define type-safe interfaces for:
- **Node Types**: Person, Organization, Publication, Project, Serial
- **Relationship Types**: AUTHORED, INVOLVED_IN, AFFILIATED, PARTNER, etc.

### Streaming Architecture
- **Elasticsearch Scroll API**: Efficient retrieval of large result sets
- **Batch Processing**: Process data in chunks (default 1000 records)
- **Two-Pass Relationship Processing**: Import nodes first, then relationships
- **Memory Usage**: O(batch_size) instead of O(total_records)

### Data Sources
- **Elasticsearch Indexes** (ES 6.8.23):
  - `research-persons-static` (195K documents)
  - `research-organizations-static` (18K documents) 
  - `research-publications-static` (95K documents)
  - `research-projects-static` (6.8K documents)
  - `research-serials-static` (40K documents)

## Directory Structure

```
├── src/
│   ├── protocols/           # Type protocols for nodes and relationships
│   │   ├── nodes.py
│   │   └── relationships.py
│   ├── es_client/          # ES connectivity and streaming extraction
│   │   ├── client.py       # ES client with scroll API support
│   │   ├── base_extractor.py # Base streaming extractor
│   │   └── extractors.py   # Entity-specific extractors
│   ├── models/             # Concrete implementations of protocols
│   ├── graph_db/           # Neo4j integration and management
│   │   ├── connection.py   # Database connection
│   │   ├── schema.py       # Schema and constraint management
│   │   ├── db_manager.py   # Safe database operations
│   │   ├── importer.py     # Legacy import pipeline
│   │   └── streaming_importer.py # Streaming import pipeline
│   └── validation/         # Data validation utilities
├── tests/
│   ├── unit/               # Unit tests for individual components
│   └── integration/        # Integration tests for full pipeline
├── neo4j_cli.py           # CLI interface for all operations
├── docs/                   # Additional documentation
└── requirements.txt        # Python dependencies
```

## Installation & Setup

### 1. Clone and Setup
```bash
# Clone repository
git clone <repository-url>
cd demo_small_neo4j

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Environment Variables
Create a `.env` file in the project root with your connection details:
```bash
# Elasticsearch Configuration
ES_HOST=your_elasticsearch_host
ES_USER=your_username
ES_PASS=your_password

# Neo4j Configuration  
NEO4J_URI=neo4j+s://your_instance.databases.neo4j.io
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_password
NEO4J_DATABASE=neo4j

# Optional: OpenAI for embeddings
OPENAI_API_KEY=your_openai_key
```

### 3. Verify Setup
```bash
# Test connections and scroll API
python neo4j_cli.py test

# Extract sample data for formatting development (optional)
python neo4j_cli.py format extract-samples all --samples 5

# Run tests
python -m pytest tests/
```

## CLI Quick Reference

### Connection & Setup
```bash
python neo4j_cli.py test
```

### Schema Management
```bash
python neo4j_cli.py schema setup
python neo4j_cli.py schema info
python neo4j_cli.py schema reset
```

### Database Operations
```bash
python neo4j_cli.py db stats
python neo4j_cli.py db clear [--force]
python neo4j_cli.py db clear-type Person,Publication [--force]
python neo4j_cli.py db validate
```

### Data Import
```bash
# Validation and testing
python neo4j_cli.py import dry-run [--size SIZE] [--batch-size BATCH_SIZE]
python neo4j_cli.py import sample [--size SIZE] [--batch-size BATCH_SIZE]

# Full import
python neo4j_cli.py import full [--batch-size BATCH_SIZE] [--entity-types ENTITY_TYPES] [--skip-relationships]

# Relationships only
python neo4j_cli.py import relationships [--types TYPES] [--batch-size BATCH_SIZE]
```

### Document Formatting (Development)
```bash
# Sample management
python neo4j_cli.py format extract-samples ENTITY_TYPE [--samples COUNT] [--force-refresh]
python neo4j_cli.py format list-cache
python neo4j_cli.py format clear-cache ENTITY_TYPE

# Structure debugging
python neo4j_cli.py format test ENTITY_TYPE [--index INDEX] [--show-original] [--show-formatted] [--show-both] [--verbose]
python neo4j_cli.py format evaluate ENTITY_TYPE [--samples COUNT] [--detailed] [--force-refresh]
python neo4j_cli.py format compare ENTITY_TYPE [--count COUNT]
```

### Interactive Development
```bash
python format_test_runner.py [--test ENTITY_TYPE] [--extract ENTITY_TYPE] [--batch] [--interactive]

# Interactive commands:
# extract ENTITY_TYPE [count]
# show [original|formatted|both] ENTITY_TYPE [index]
# test ENTITY_TYPE [index]
# cache
# quit
```

### Value References
- **ENTITY_TYPE**: `persons`, `organizations`, `publications`, `projects`, `serials`, `all`
- **TYPES**: `AFFILIATED`, `AUTHORED`, `INVOLVED_IN`, `PARTNER`, `PUBLISHED_IN`, `PART_OF`

### Document Formatting Workflow
The formatting system lets you debug document transformations without database operations:

1. **Extract samples** → Test formatting → **Iterate safely**
2. **`--show-original`**: See raw ES structure
3. **`--show-formatted`**: See transformed Neo4j structure  
4. **`--show-both`**: Compare before/after with field changes

**Safe to change**: All fields except `es_id` (required for Neo4j operations). The formatting protocol is the source of truth.

## Batch Size Recommendations

| Memory Available | Dataset Size | Recommended Batch Size | Estimated Time |
|-----------------|--------------|------------------------|----------------|
| < 8GB | Any | 500-1000 | Slower but safe |
| 8-16GB | < 500K | 1000-2000 | Balanced |
| 8-16GB | > 500K | 2000-3000 | Good performance |
| 16-32GB | < 1M | 3000-5000 | Fast |
| 16-32GB | > 1M | 5000-10000 | Very fast |
| > 32GB | Any | 10000-20000 | Maximum speed |

## Key Components

### Streaming Extractors (`src/es_client/`)
- **BaseStreamingExtractor**: Abstract base class for streaming extraction
- **Entity Extractors**: Specialized extractors for each entity type
- **Batch Processing**: Yields data in configurable chunks
- **Backward Compatible**: Supports both streaming and legacy modes

### Elasticsearch Client Enhancement
```python
# New scroll API support
def scan_documents(self, index: str, query: Dict[str, Any] = None, 
                   batch_size: int = 1000, scroll_timeout: str = '5m'):
    """Generator that yields batches of documents using scroll API"""
```

### Streaming Import Pipeline
- **Memory Efficient**: Processes batches without loading all data
- **Progress Tracking**: Real-time import statistics with ETA
- **Checkpoint Support**: Resume imports after failures
- **Two-Pass Strategy**: Import nodes first, then relationships

## Performance Optimization

### Memory Usage
- **Legacy Mode**: O(n) where n = total records
- **Streaming Mode**: O(b) where b = batch size
- **With Bloom Filters**: O(b + m) where m = bloom filter size

### Import Speed
- Batch processing reduces Neo4j transaction overhead
- Concurrent processing for independent entity types
- Efficient relationship creation using cached IDs

### Error Recovery
- Checkpoint system saves progress
- Resume from last successful batch
- Validation before import prevents bad data

## Development Workflow

### Phase 1: Protocols & ES Integration ✅
- [x] Define all node and relationship protocols
- [x] Implement Elasticsearch client with ES 6.8.23 compatibility
- [x] Create data extractors with filtering capabilities
- [x] Add scroll API support for streaming
- [x] Update extractors to support batch processing

### Phase 2: Streaming Architecture ✅
- [x] Implement BaseStreamingExtractor
- [x] Update all extractors to support streaming
- [x] Add batch size configuration
- [x] Maintain backward compatibility

### Phase 3: Neo4j Streaming Import 🚧
- [x] Implement StreamingImportPipeline
- [x] Add progress tracking with ETA
- [ ] Fix publication document formatting issue
- [ ] Implement checkpoint system
- [ ] Add memory-efficient relationship processing

### Phase 4: Graph RAG ❌
- [ ] Embedding generation pipeline
- [ ] Vector similarity search
- [ ] Graph traversal for context expansion

## Current Limitations

### No Checkpoint/Recovery
If an import fails, it must be restarted from the beginning.

### Embedding Pipeline
OpenAI embedding generation and vector search functionality not yet implemented.

## Monitoring Import Progress

The streaming import provides real-time statistics:
```
📊 Progress: 45.2% | Rate: 5234 items/sec | ETA: 0:02:35
```

## Contributing

1. **Code Style**: Follow PEP 8, use type hints
2. **Testing**: Write tests for new functionality
3. **Documentation**: Update README and docstrings
4. **Streaming**: Ensure new features support batch processing

## License

[Your License]

## Contact

[Your Contact Information]