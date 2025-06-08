# Delta Encoding for Columnar Time-Series Data (Go)

This Go program showcases how **Delta Encoding** with **checkpointing** can optimize storage and retrieval of time-series data in a columnar format. It simulates how database internals—especially in time-series databases (TSDBs)—compress and manage numerical metrics like CPU or memory usage over time.

## Table of Contents

1. [What is Delta Encoding?](#what-is-delta-encoding)
2. [Why Use Delta Encoding in Databases?](#why-use-delta-encoding-in-databases)
3. [How Does Delta Encoding Work in This Code?](#how-does-delta-encoding-work-in-this-code)
4. [Trade-Offs and Design Decisions](#trade-offs-and-design-decisions)
5. [Assumptions and Abstractions](#assumptions-and-abstractions)
6. [Running the Code](#running-the-code)
7. [Key Features](#key-features)
8. [Future Improvements](#future-improvements)

---

### What is Delta Encoding?

**Delta Encoding** compresses a sequence of values by storing the difference between consecutive values rather than the raw values themselves.

#### Example:

Original values:

```
[100, 102, 105, 105, 110]
```

Delta encoded:

```
[100, +2, +3, 0, +5]
```

Only the base value (100) and the differences are stored. For monotonic or slowly changing sequences—like metrics in time—this saves significant space.

---

### Why Use Delta Encoding in Databases?

1. **Space Efficiency**:

   * Ideal for slowly changing or periodic metrics (e.g., CPU usage every 2 seconds).
   * Delta values are often smaller numbers that compress well with VarInt encoding.

2. **Optimized for Time-Series**:

   * Works best when `ts` (timestamps) are monotonically increasing and values change in small steps or with noise.

3. **Checkpointing for Fast Access**:

   * To avoid full-sequence traversal on lookup, checkpoints are inserted periodically to store absolute values. This speeds up random access to specific rows.

4. **Better Cache Performance**:

   * Smaller encoded arrays improve cache locality for queries scanning large sequences.

---

### How Does Delta Encoding Work in This Code?

The dataset consists of rows with the following fields: `id`, `value` (e.g., memory in bytes), and `ts` (timestamp). The columns are stored separately in columnar style.

#### Core Structures:

* `idList`: Stores raw `id` values.
* `deltaValueList`: Stores delta values of the `value` column.
* `deltaTsList`: Stores time difference between current and previous timestamps.
* `checkpointValues`: Every N values, the absolute `value` is stored here.
* `checkpointTs`: Stores the absolute `ts` at each checkpoint.
* `originalRows`: Preserved for correctness checks.

#### Key Operations:

* **appendRow**:

  * Encodes incoming rows using deltas from the previous value/timestamp.
  * Inserts checkpoints every N rows (configurable).

* **reconstructRow**:

  * Reconstructs a row using the nearest prior checkpoint, then adds deltas up to the target row index.

* **verifyDeltaEncodingCorrectness**:

  * Rebuilds the entire table and compares it to the original. A full equality check ensures data integrity.

* **printStats**:

  * Calculates compressed size using simulated VarInt encoding and compares with original uncompressed size.

---

### Trade-Offs and Design Decisions

#### Delta-from-Previous vs Delta-from-Base

We opted for **delta-from-previous** over **delta-from-base** for these reasons:

* **Smaller Deltas**: When values change slowly, consecutive values are close, producing smaller deltas (better VarInt compression).
* **Simpler Checkpoint Logic**: With delta-from-previous, we only need to store one previous value during encoding.
* **Decoding Overhead**: Requires accumulating deltas during decoding (linear scan after the last checkpoint).

In contrast, delta-from-base makes decoding slightly faster (by multiplying the delta and adding to base) but performs worse in compression when values fluctuate around a trendline.

#### Why Not Use Delta-of-Delta?

**Delta-of-Delta encoding** captures the *change in change*, i.e., second-order deltas. It’s powerful for highly regular sequences like fixed-step timestamps.

We didn't use it here because:

* Our `value` column had minor jitter—delta-of-delta would increase the number of outliers.
* Simplicity was a priority.
* For timestamps, plain delta and checkpoints provided a good enough tradeoff.

Still, delta-of-delta is great for:

* Monotonic sequences like timestamps with fixed intervals (e.g., 2s step size).
* Reducing even further the variance of the encoded sequence.

#### Use of VarInt Encoding

After delta encoding, we simulate applying **VarInt encoding** (from `binary.PutVarint`) to calculate space savings:

* VarInt encodes small integers into fewer bytes.
* Since deltas are smaller than raw values, they compress better.
* This reflects real-world storage engines where encoded deltas are VarInt-packed on disk.

---

### Assumptions and Abstractions

* **Sorted Timestamps**:

  * `ts` column is assumed to be sorted. Common in TSDBs, where insert order follows time.

* **Columnar Storage**:

  * Each column (`id`, `value`, `ts`) is stored as a separate slice to mimic columnar DB layout.

* **Checkpointing**:

  * Checkpoints are added every N rows (default = 4). They store full values and timestamps to allow faster decoding.

* **No Deletes/Updates**:

  * The model assumes only appends—realistic for TS workloads where past entries are immutable.

* **Compression Evaluation**:

  * VarInt encoding (simulated) is used to estimate compressed sizes.

---

### Running the Code

#### 1. Prerequisites

Make sure Go 1.18+ is installed:

```bash
go version
```

#### 2. Run the Program

```bash
go run main.go
```

#### 3. Output

You’ll see:

* Sample reconstructions for selected rows.
* A correctness check.
* Compression statistics (original vs delta-encoded size).

Example output:

```
&{1 10737418240 1000}
&{3 10758390272 1004}
&{5 10727939072 1008}
&{10 10569646080 1018}

Is delta encoding correct: true

Varint Encoded Sizes:
Total compressed size (varint): 52 bytes
Original size (varint): 80 bytes
Saved: 28 bytes (35.00%)
```

---

### Key Features

* Delta encoding with efficient integer storage.
* Checkpointing to balance compression vs decoding speed.
* Full row reconstruction from compressed data.
* Compression stats for measuring effectiveness.
* Correctness validation against original rows.

---

### Future Improvements

1. **Support for Floats**:

   * Extend delta encoding to support float64 values with tolerance for precision loss.

2. **Range Queries**:

   * Add ability to reconstruct a slice of rows efficiently instead of one-by-one.

3. **Benchmarking**:

   * Compare compression ratio and speed against other encoding schemes like RLE, Gorilla, or Bitpacking.

4. **Generic Compression Layer**:

   * Abstract the encoding interface to allow plug-and-play with different strategies.

5. **Persistent Storage**:

   * Add read/write to disk support (using Protobuf or FlatBuffers) for real-world usage.
