# NoSQL Database for Credit Card Fraud Detection

## Objective

This project implements a graph-based NoSQL database system designed to detect credit card fraud patterns using Neo4j. The system simulates realistic transaction data between customers and terminals, loads it into a graph database, and executes analytical queries to identify suspicious activities and customer relationships.

The main goals of this project are:

- Generate synthetic datasets of varying sizes (50MB, 100MB, 200MB) containing customer profiles, terminal profiles, and transaction records with simulated fraud scenarios
- Design and implement a graph data model in Neo4j that efficiently represents relationships between customers, terminals, and transactions
- Develop and optimize Cypher queries for fraud detection analytics, including identifying similar spending patterns, detecting potentially fraudulent transactions, and discovering co-customer relationships
- Extend the data model with additional transaction attributes (time period, product type, security feeling) and create "buying friends" relationships based on shared terminal usage and similar security ratings
- Evaluate and compare performance metrics across different dataset sizes

## Technologies Used

### Database
- **Neo4j** - Graph database management system chosen for its ability to efficiently model and traverse complex relationships between entities

### Programming Languages & Libraries
- **Python** - Primary programming language for data generation, loading, and query execution
- **neo4j** - Official Python driver for Neo4j database connectivity
- **pandas** - Data manipulation and analysis library for handling datasets
- **pandarallel** - Parallel processing extension for pandas to accelerate data generation
- **numpy** - Numerical computing library for statistical operations and random data generation
- **matplotlib** - Data visualization library for generating performance charts
- **seaborn** - Statistical data visualization built on matplotlib

### Neo4j Extensions
- **APOC** (Awesome Procedures on Cypher) - Library providing additional procedures and functions including:
  - `apoc.periodic.iterate` - Batch processing for large-scale data operations
  - `apoc.path.expandConfig` - Configurable path expansion for graph traversal
  - `apoc.coll.combinations` - Collection manipulation for generating customer pairs
  - `apoc.map.fromPairs` - Map creation utilities

### Data Formats
- **Pickle (.pkl)** - Binary format for storing Python objects (customer and terminal data)
- **CSV** - Text format for bulk loading transactions into Neo4j

## Performance

### Data Loading Performance

The loading operations demonstrate linear scalability across dataset sizes, with the majority of time spent on transaction relationship creation:

| Dataset | Transactions | Customer Nodes | Terminal Nodes | Available Terminals | Made Transaction |
|---------|-------------|----------------|----------------|---------------------|------------------|
| 50MB    | 1,005,086   | 0.26s          | 0.03s          | 0.26s               | 10.64s           |
| 100MB   | 2,010,163   | 0.10s          | 0.01s          | 0.16s               | 19.81s           |
| 200MB   | 4,000,565   | 0.08s          | 0.01s          | 0.19s               | 40.92s           |

Key optimizations implemented:
- Batch processing using `UNWIND` for node creation
- `LOAD CSV` with `IN TRANSACTIONS OF 10000 ROWS` for scalable transaction loading
- Uniqueness constraints and indexes on frequently queried attributes

### Query Performance

Query execution times across the three datasets (with k=3, customer_id=6 for CoCustomersK):

| Query | 50MB | 100MB | 200MB | Description |
|-------|------|-------|-------|-------------|
| SimilarSpenders | 0.71s (2,725 records) | 1.00s (5,828 records) | 1.76s (9,297 records) | Find customers with similar spending on shared terminals |
| FraudulentTransactions | 0.56s (18,943 records) | 0.63s (18,925 records) | 1.56s (18,886 records) | Identify transactions exceeding 120% of terminal average |
| CoCustomersK | 0.05s (297 records) | 0.15s (448 records) | 0.40s (563 records) | Find co-customers at degree k via path expansion |
| ExtendsBuyingF | 5.87s | 9.22s | 30.57s | Extend transactions and create buying_friends relationships |
| PeriodFraudStats | 0.29s (4 records) | 0.41s (4 records) | 1.75s (4 records) | Aggregate fraud statistics by time period |

### Performance Observations

- **SimilarSpenders, FraudulentTransactions, and PeriodFraudStats** queries show near-linear scaling with dataset size, maintaining sub-2 second response times even for the 200MB dataset
- **CoCustomersK** demonstrates excellent performance due to efficient path traversal using `apoc.path.expandConfig`, consistently executing under 0.5 seconds
- **ExtendsBuyingF** is the most computationally intensive operation due to:
  - Two nested `apoc.periodic.iterate` operations
  - Parallel batch processing of millions of transactions
  - Combinatorial pair generation for buying_friends relationships
  - Execution time scales from ~6s (50MB) to ~31s (200MB)

### Optimization Strategies Applied

1. **Indexing** - Created indexes on `amount`, `datetime`, and `nb_terminals` for faster lookups
2. **Batch Processing** - Used configurable batch sizes (500-10,000 rows) for memory-efficient bulk operations
3. **Parallel Execution** - Enabled parallel processing in APOC procedures where safe
4. **Subquery Isolation** - Used `CALL {}` blocks to compute aggregates once and reuse results
5. **Map-based Lookups** - Pre-computed terminal averages into maps for O(1) access during fraud detection
