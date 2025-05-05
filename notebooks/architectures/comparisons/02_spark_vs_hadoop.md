# Hadoop (MapReduce) vs Apache Spark

Hadoop (MapReduce) was the first widely adopted open‑source big‑data engine, designed for large‑scale **batch** processing on HDFS.  
Apache Spark is a newer, in‑memory–centric engine that supports **batch, streaming, interactive, and graph** workloads.

---

## 1  Core Purpose  

* **Hadoop (MapReduce)** – disk‑based, fault‑tolerant batch processing.  
* **Spark** – in‑memory unified engine for fast batch, real‑time, and iterative analytics.

---

## 2 Architecture & Execution  

### 2.1 Execution Model  
* **Hadoop**: JobTracker → splits job into map tasks → shuffle to reducers → writes results to HDFS.  
* **Spark**: Driver builds a DAG → divides into stages → tasks run in memory; spills only when needed.

### 2.2 Storage Layer  
* **Hadoop** tightly couples to **HDFS** (replicated blocks on disk).  
* **Spark** can read/write HDFS, S3, GCS, Cassandra, Kafka, JDBC, and more.

---

## 3 Performance  

* **Hadoop**: each map/reduce phase reads from and writes to disk → high latency.  
* **Spark**: keeps intermediate data in memory → 10‑100× faster for iterative or interactive workloads.

---

## 4 API & Workload Coverage  

| Category         | Hadoop (MapReduce)                   | Spark (Core + Libraries)                     |
|------------------|--------------------------------------|---------------------------------------------|
| Batch            | Yes                                  | Yes (Spark Core, Spark SQL)                 |
| Stream           | Limited (Apache Storm / Flink add‑ons)| Yes (Spark Structured Streaming)            |
| SQL              | Hive (separate engine)               | Built‑in Spark SQL                          |
| Machine Learning | Mahout (MapReduce‑based)             | MLlib (in‑memory, faster)                   |
| Graph            | Giraph                               | GraphX                                     |
| Interactive      | Pig / Hive CLI                       | Spark Shell, Spark Notebook                |

---

## 5 Cost & Complexity  

* **Hadoop**: inexpensive storage, but longer compute runtimes → more cluster hours.  
* **Spark**: higher memory requirements per node, but finishes jobs faster — often lower total compute cost.

---

## 6 Fault Tolerance  

* **Both** rely on data replication (HDFS) or lineage (Spark RDDs) for recovery.  
* Spark recomputes lost partitions from DAG lineage; Hadoop re‑runs failed tasks on replica data blocks.

---

## 7 Detailed Feature Table  

| Feature           | **Hadoop (MapReduce)**                          | **Apache Spark**                                      |
|-------------------|-------------------------------------------------|-------------------------------------------------------|
| Processing Model  | Two‑stage Map → Shuffle → Reduce                | DAG of stages (in‑memory)                             |
| Primary Storage   | HDFS                                            | HDFS, S3, GCS, Cassandra, etc.                        |
| Speed             | Disk‑bound, slower                              | In‑memory; 10‑100× faster on iterative jobs           |
| Workloads         | Batch ETL                                       | Batch, Streaming, SQL, ML, Graph                      |
| Data Streaming    | Limited (add‑ons)                               | First‑class Structured Streaming                      |
| Cost Model        | Cheap storage, more compute hours               | Fewer compute hours; higher memory cost per node      |
| Complexity        | Higher (Java APIs, job configuration)           | Lower (high‑level APIs in Scala, Python, SQL)         |
| Fault Tolerance   | High (data replication)                         | High (RDD lineage + checkpointing)                    |
| Ecosystem         | Hive, Pig, Mahout, Giraph                       | Spark SQL, MLlib, GraphX, Structured Streaming        |
| Typical Use Cases | Archival batch processing, nightly ETL          | Real‑time analytics, machine learning, interactive ETL|

---

## 8 Key Takeaways  

* **Choose Hadoop (MapReduce)** when you primarily need **cheap, reliable batch ETL** over enormous datasets and can tolerate higher latency.  
* **Choose Spark** when you require **faster, in‑memory analytics**, unified batch & streaming, or iterative algorithms (ML, graph) across big data.
