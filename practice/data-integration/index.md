# Data Integration Practice

## Overview

This practice module introduces you to **data integration** - the process of combining data from different sources into a unified, meaningful, and valuable dataset. Data integration is essential in modern data ecosystems where information is scattered across multiple systems, formats, and locations.

## Learning Objectives

By completing these exercises, you will:

1. **Understand Data Integration Concepts**
   - Learn what data integration is and why it matters
   - Explore common data integration patterns (ETL, ELT, streaming)
   - Understand data quality and transformation challenges

2. **Master Apache NiFi**
   - Build visual data flows using NiFi's drag-and-drop interface
   - Work with processors to ingest, transform, and route data
   - Monitor and debug data pipelines in real-time

3. **Apply Real-World Skills**
   - Extract data from multiple file sources
   - Clean and validate data quality
   - Merge datasets from different sources
   - Load processed data into HDFS for storage and analytics

## Prerequisites

- Basic understanding of data formats (CSV, TXT)
- Familiarity with Docker and docker-compose
- Access to the provided Hadoop stack environment

## Practice Modules

### [TP1 - Apache NiFi Introduction](tp1-nifi-intro.md)

Get started with Apache NiFi through a simple "Hello World" example. Learn to:
- Access the NiFi UI
- Add and configure processors
- Create your first data flow
- Monitor flow execution

**Time:** 30-45 minutes
**Difficulty:** Beginner

### [TP2 - NiFi Real-World Use Case](tp1-nifi-use-case.md)

Apply your NiFi knowledge to a realistic scenario using employee data. You will:
- Import data from multiple CSV files
- Clean and validate dates
- Merge related datasets (employees, functions, salaries)
- Store the final integrated dataset in HDFS

**Time:** 1-2 hours
**Difficulty:** Intermediate

## Environment Setup

Ensure your Hadoop stack is running with the NiFi profile:

```bash
# Start the stack with NiFi
docker-compose --profile nifi up -d

# Verify NiFi is running
docker ps | grep nifi

# Access NiFi UI
open https://localhost:8443/nifi
```

**Credentials:**
- Username: `admin`
- Password: `adminadminadmin`

## What is Data Integration?

Data integration combines technical and business processes to extract data from various sources, transform it into a consistent format, and load it into a target system for analysis or operational use.

### Common Use Cases

- **Business Intelligence**: Combining sales, inventory, and customer data for reporting
- **Data Warehousing**: Consolidating data from multiple operational systems
- **Master Data Management**: Creating a single source of truth for entities like customers or products
- **Real-time Analytics**: Streaming data from IoT devices, logs, or applications

### Key Challenges

- **Data Quality**: Inconsistent formats, missing values, duplicates
- **Schema Evolution**: Source systems change over time
- **Performance**: Moving large volumes of data efficiently
- **Reliability**: Ensuring data flows are fault-tolerant

## Tools in This Module

### Apache NiFi

An easy-to-use, powerful, and reliable system to process and distribute data. NiFi provides:

- **Visual Design**: Drag-and-drop interface for building data flows
- **Data Provenance**: Track data from source to destination
- **Extensibility**: 300+ processors for various data operations
- **Scalability**: Distributed architecture for high-volume processing

### HDFS (Hadoop Distributed File System)

A distributed storage system designed to store large datasets reliably. Benefits include:

- **Fault Tolerance**: Data is replicated across nodes
- **Scalability**: Store petabytes of data
- **High Throughput**: Optimized for large sequential reads/writes

## Getting Help

- [Apache NiFi Documentation](https://nifi.apache.org/docs.html)
- [NiFi Getting Started Guide](https://nifi.apache.org/docs/nifi-docs/html/getting-started.html)
- [HDFS Architecture](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)

## Next Steps

Start with [TP1 - Apache NiFi Introduction](tp1-nifi-intro.md) to build your first data flow!
