# Spark Essentials
This repository contains a guide to Apache Spark, covering various topics from fundamentals to advanced concepts.

## Overview
- [Introduction](01%20Introduction.md): 
  - This section introduces Apache Spark, discussing its advantages over traditional systems like MapReduce, its history, and key features. 
  - It explains Spark's architecture, detailing high-level libraries, Spark Core, and cluster managers, along with the driver and worker components. Core concepts like SparkSession, dataflow, cluster management, and execution are covered, concluding with Kubernetes integration.

- [Resilient Distributed Dataset (RDD)](02%20RDD.md): 
  - Defines RDDs, highlighting their characteristics and the difference between transformations and actions. 
  - It outlines key features, various operations (creation, transformation, action), and provides examples, including word count and join operations. Execution methods for Spark applications are also discussed.

- [Spark SQL](03%20Spark%20SQL.md): 
  - Spark SQL is introduced with its features, component stack, and Hive integration. 
  - It covers DataFrames and Datasets, their creation, evolution, and usage in PySpark and Scala. 
  - The section explains SQL context, SparkSession, user-defined functions, query optimization, data partitioning, skew, caching, and Spark 3 improvements.

- [Spark Streaming](04%20Spark%20Streaming.md): 
  - This section highlights the importance of real-time processing, describing DStreams, their sources, and the integration with batch code. 
  - It compares DStreams with structured streaming, covering windowing, watermarking, output modes, stream processing, triggers, continuous processing, and fault tolerance. 
  - Kafka connectors and their usage are also discussed.

- [Delta Lake](05%20Delta%20Lake.md): 
  - Delta Lake addresses common data lake challenges with features like transaction logs, ACID transactions, and conflict resolution. 
  - It covers advanced functionalities like time travel, schema enforcement, upsert operations, and constraints. 
  - Change Data Feed, optimization techniques, integrations with other platforms, future developments, and the lakehouse concept are also explored.


## Notebooks
This repository also contains a folder called `Notebooks`, which includes Jupyter notebooks with example Spark code. These notebooks have been developed and executed in [Databricks](https://www.databricks.com/). The `Notebooks` folder contains the following files:


- [0 Spark Introduction.ipynb](./Notebooks/0%20Spark%20Introduction.ipynb)
   - RDD
   - Transformations: ReduceByKey, SortByKey, Filter
   - Key/Value Pair RDD

- [1 Spark SQL.ipynb](./Notebooks/1%20Spark%20SQL.ipynb)
   - Create Dataframe
   - Create Temporary View
   - Execute SQL query
   - Load CSV to dataframe
   - Aggregation Functions: Average, most common, count
   - User-Defined Functions (UDFs)

- [2 Spark SQL Advanced.ipynb](./Notebooks/2%20Spark%20SQL%20Advanced.ipynb)
   - Window partitioning
   - Data Cleaning (e.g. drop null values)
   - Joins: optimizations, repartitions, etc.
   - Adaptative Query Execution (AQE): coalesce partitions, tail, etc.

- [3 Spark Streaming.ipynb](./Notebooks/3%20Spark%20Streaming.ipynb)
   - Structured streaming
   - Windowing
   - Stream Join

- [4 Spark Streaming - Kafka.ipynb](./Notebooks/4%20Spark%20Streaming%20-%20Kafka.ipynb)
   - Structured Streaming with Apache Kafka
   - Sliding Window

- [5 Spark SQL Delta Lake.ipynb](./Notebooks/5%20Spark%20SQL%20Delta%20Lake.ipynb)
   - ACID Transactions
   - Time Travel: history, timestamp
   - Schema Evolution
   - Constraints
   - Upsert and Merge
   - Change Data Feed

- [Final Project.ipynb](./Notebooks/Final%20Project.ipynb)

Additionally, there is a subfolder `Notebooks_with_outputs`, which contains the executed versions of these notebooks, allowing you to view the outputs obtained.



## How to Use This Repository
Each section in this repository corresponds to a markdown file (.md) covering specific topics related to Spark essentials. You can navigate through the overview above to access the content you're interested in.

Feel free to explore the files, follow along with the explanations, and try out the exercises provided to deepen your understanding of Spark concepts and tools.

## Contributing

Contributions to this repository are welcome! If you have suggestions for improving existing content, want to add new topics, or spot any errors, please feel free to open an issue or submit a pull request.

Have a great Spark journey! 🚀🌟

