# Spark Terminology — Key Concepts (most → least important)

| Spark Concept | Simple Meaning | Everyday Analogy |
|---------------|----------------|------------------|
| **Driver** | Your main Spark program; plans all work and assigns it. | The head chef who designs the menu and tells each kitchen what to cook. |
| **Executor** | A runtime process on each machine that actually runs tasks. | A kitchen crew cooking the assigned dishes. |
| **Task** | A tiny slice of work (e.g., process one data partition) sent to an executor. | One chef chopping one pile of vegetables. |
| **Stage** | A group of tasks that can run in parallel before any data has to move. | All kitchens finish chopping before anyone starts cooking. |
| **Shuffle** | The step where data moves between executors/nodes for the next stage. | Passing half‑prepped veggies from one kitchen to another. |
| **Cluster** | The whole group of machines working together. | A restaurant with many kitchens under one roof. |
| **Node / Worker** | One computer inside the cluster that hosts executors. | A single kitchen in the restaurant. |

# Spark Learning Path

## Level 0 — Installation  
- Install Spark locally or via a package manager.  
- Launch **Spark Shell** (`spark-shell` for Scala) or **PySpark** (`pyspark`) to verify the install.

---

## Level 1 — Basics of Spark APIs  
- Understand the three core abstractions: **RDDs**, **DataFrames**, **Datasets**.  
- Master common **Transformations**: `map`, `filter`, `groupBy`, `join`.  
- Use **Actions**: `count`, `show`, `collect`.  
- Remember **Lazy Execution**: Spark builds a plan and runs only when an action is invoked.

---

## Level 2 — Optimisation  
- Learn the **Catalyst Optimizer** (Spark’s query optimiser).  
- Pick the right **columnar storage** format (Parquet > CSV/JSON for analytics).  
- **Reduce shuffles** triggered by `groupBy`, `orderBy`, `sort`.  
- Know **`coalesce` vs `repartition`** (combine vs split partitions).  
- **Avoid UDFs** unless critical; they bypass Catalyst and slow execution.

---

## Level 3 — Performance Tuning  
- Use **Broadcast joins** when one side is small.  
- Adjust the **broadcast‑join threshold** (`spark.sql.autoBroadcastJoinThreshold`).  
- Tune **shuffle partitions** (`spark.sql.shuffle.partitions`) for optimal parallelism.  
- Handle skewed data with **Adaptive Query Execution** (Spark 3 +).  
- Read `.explain()` plans to diagnose performance bottlenecks.

---

## Level 4 — Cluster Management  
- Explore deployment modes: **YARN**, **Kubernetes**, **Standalone**, **EMR**, **Databricks**.  
- Configure **Dynamic Allocation** to scale executors up/down automatically.  
- Always know both **RDD** and **DataFrame** APIs, but **prefer DataFrames** for performance.

--- 

# Level 1 — Basics of Spark APIs (Detailed)

## 1.1 Core Abstractions  

| Abstraction | What It Is | When to Use |
|-------------|-----------|-------------|
| **RDD (Resilient Distributed Dataset)** | Low‑level distributed collection of Java/Python objects. Gives fine‑grained control and type freedom. | Legacy code, custom partition logic, advanced tuning. |
| **DataFrame** | Distributed table with named columns; backed by Catalyst optimiser. Similar to a SQL table. | Day‑to‑day analytics and ETL. Fastest and most feature‑rich. |
| **Dataset** *(Scala/Java only)* | DataFrame + compile‑time type safety (strongly typed objects). | When you need both Catalyst optimisation **and** typed records. |

> **Best practice:** default to **DataFrames**; drop to RDDs only for edge cases.

---

## 1.2 Transformations (build the plan, **no execution yet**)  

| Transformation | Purpose | Mini‑Example |
|----------------|---------|--------------|
| `map`          | Row‑by‑row conversion | `df.rdd.map(lambda r: r.age + 1)` |
| `filter`       | Row selection          | `df.filter(col("age") > 18)` |
| `groupBy`      | Aggregate keys         | `df.groupBy("city").count()` |
| `join`         | Combine two DataFrames | `df1.join(df2, "id", "inner")` |

Transformations return **new immutable DataFrames/RDDs**; Spark just logs them in a DAG.

---

## 1.3 Actions (trigger computation)  

| Action   | What It Does               | Example |
|----------|---------------------------|---------|
| `count`  | Returns row count         | `df.count()` |
| `show`   | Pretty‑prints first rows  | `df.show(5)` |
| `collect`| Brings all rows to driver | `df.collect()` ⚠️ (use carefully on big data) |

Once an action fires, Spark optimises the DAG, allocates resources, and runs tasks.

---

## 1.4 Lazy Execution (Why It Matters)  
- **Build phase:** Transformations add steps to a DAG → *no data moves yet*.  
- **Trigger phase:** First action launches the job; Spark optimises, combines, and reorders steps for best performance.  
- **Benefit:** Eliminates unnecessary work (e.g., unused columns) and fuses steps into fewer passes over the data.

> **Rule of thumb:** Chain many transformations freely; Spark will run them efficiently once you call an action.
