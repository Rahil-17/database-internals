# Run-Length Encoding (RLE) for Columnar Data Storage

This Go program demonstrates how **Run-Length Encoding (RLE)** can be applied to optimize the storage and querying of columnar data, particularly for time-series (TS) data. The solution is designed to make repetitive data storage efficient and enable fast access, often used in **database internals** for time-series databases (TSDBs) or analytical systems.

## Table of Contents
1. [What is Run-Length Encoding (RLE)?](#what-is-run-length-encoding-rle)
2. [Why Use RLE in Databases?](#why-use-rle-in-databases)
3. [How Does RLE Work in This Code?](#how-does-rle-work-in-this-code)
4. [Assumptions and Abstractions](#assumptions-and-abstractions)
5. [Running the Code](#running-the-code)
6. [Key Features](#key-features)
7. [Future Improvements](#future-improvements)

---

### What is Run-Length Encoding (RLE)?

**Run-Length Encoding (RLE)** is a simple form of data compression where consecutive occurrences of the same value (called a "run") are stored as a single data value along with a count. In the context of time-series data or databases, RLE helps to significantly reduce the storage requirements when there are sequences of repeated values.

#### Example:
For example, if you have a list like:
```

[1, 1, 1, 2, 2, 3, 3, 3, 3]

```
RLE would store it as:
```

[(1, 3), (2, 2), (3, 4)]

```

This compact representation avoids redundant storage of the same value multiple times, optimizing both space and potentially query performance for certain operations.

---

### Why Use RLE in Databases?

1. **Space Efficiency**:
   - Repetitive data takes up less space. For columns that store repetitive or similar values (e.g., timestamps in time-series data), RLE can reduce the storage footprint.

2. **Improved Query Performance**:
   - If data is compressed into runs of identical values, many queries—especially those that focus on distinct values or ranges—can be executed faster by scanning fewer entries.

3. **Time-Series Data**:
   - In time-series databases, RLE is often used to efficiently store repeating timestamps or other columns with similar repetitive values, as these types of data are prevalent in metrics, logs, and monitoring systems.

4. **Compression**:
   - By encoding the data efficiently, it reduces I/O for disk operations, which is crucial for large datasets in analytical queries.

---

### How Does RLE Work in This Code?

In this Go implementation, RLE is applied to time-series data. The program simulates a columnar database structure for a table with three columns: `id`, `value`, and `timestamp (ts)`.

- **Data Structure**:
  - `idList`: Stores `id` values.
  - `valueList`: Stores `value` values.
  - `tsRuns`: Stores unique timestamps and their counts, i.e., consecutive occurrences of the same timestamp.
  - `tsRunEnds`: A running total that allows us to track the cumulative number of entries up to a given timestamp.

- **Key Operations**:
  - **Appending Rows**: As rows are appended, the program either starts a new run for a new timestamp or increments the count for an existing timestamp.
  - **Reconstructing Rows**: The program can reconstruct rows by mapping the row ID to its corresponding `id`, `value`, and `timestamp`.
  - **Counting Occurrences**: The program can quickly count the occurrences of each unique timestamp using binary search.

---

### Assumptions and Abstractions

- **Sorted Timestamp Column**: The `ts` (timestamp) column is assumed to be sorted. This is a common assumption in many time-series databases, where data is typically inserted in chronological order.

- **Columnar Storage**: The program uses a columnar storage model, where each column is stored separately for efficient access and retrieval. This abstraction allows RLE to compress only the repetitive data in the `ts` column, without affecting other columns like `id` or `value`.

- **No Deletions or Updates**: The code does not handle updates or deletions of rows. This is a simplification, as typical databases (especially analytical ones) may only append data and rarely perform deletions or updates once data is written.

- **Prefix Sum**: The `tsRunEnds` slice keeps a cumulative count of the total number of rows across all RLE-encoded timestamps. This helps in efficiently finding the corresponding `ts` for a given row ID using binary search.

- **Binary Search**: The `getTSFromRowIDFaster` function uses binary search on the `tsRunEnds` to quickly locate the timestamp corresponding to a given row ID. This is an efficient approach for large datasets.

---

### Running the Code

1. **Prerequisites**: You’ll need **Go 1.18+** installed on your system. You can check if Go is installed by running:
```

go version

```

2. **Clone the repository** (if you're using version control):
```

git clone [https://github.com/Rahil-17/database-internals.git](https://github.com/Rahil-17/database-internals.git)
cd rle

```

3. **Run the code**:
After cloning the repository or setting up your Go file, run the program with:
```

go run main.go

```

4. **Expected Output**:
The program will print the **RLE encoding** of the time-series data, followed by the **reconstructed rows** and the results of the **point queries**.

Example:
```

RLE Encoding:
{TS: 10:00:00, Count: 2}
{TS: 10:00:02, Count: 3}
{TS: 10:00:03, Count: 1}

Full Rows:
ID  Value  TS
1   100    10:00:00
2   200    10:00:00
3   300    10:00:02
4   400    10:00:02
5   500    10:00:02
6   600    10:00:03

```

---

### Key Features

- **Efficient RLE Compression** for time-series data.
- **Binary Search** implementation for fast lookups.
- **Dynamic Row Reconstruction** based on columnar data storage.
- **Count Queries** that can quickly return the number of occurrences of a given timestamp.

---

### Future Improvements

1. **Support for Deletions/Updates**:
- Extend the system to handle updates and deletions for more realistic database operations.

2. **Persisting Data**:
- Implement file persistence to save the encoded data and load it on subsequent runs.

3. **Complex Queries**:
- Add support for more complex queries, such as range queries over time or aggregations.

4. **Compression Efficiency**:
- Evaluate and improve the space efficiency of the encoding by comparing with other compression algorithms.

5. **Scaling**:
- Benchmark with larger datasets and implement more advanced optimizations, such as parallel processing or disk-based storage.
