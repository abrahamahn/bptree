# 🏗️ B+ Tree Implementation Guide (Depth-Prefixed KV Store)

This guide walks through building a depth-prefixed B+ Tree from scratch in TypeScript. Inspired by [Okra.js](https://joelgustafson.com/posts/2023-05-04/merklizing-the-key-value-store-for-fun-and-profit), the tree is designed to work on top of any ordered key-value store (like InMemory, IndexedDB, or LMDB), supporting efficient range queries, aggregation, and later Merkle-style syncing.

---

## 🌱 Phase 1: B+ Tree on Ordered Key-Value Store

### 🎯 Goal

Implement a depth-prefixed B+ Tree that wraps a low-level ordered key-value store, exposing a clean interface for common operations:

- Fast range scans (sorted keys)
- Efficient inserts and deletes
- Tree-level key prefixes (e.g. `"00"` for leaves, `"01"` for internal nodes)

### 📦 Core Interface

```ts
type K = string;
type V = string;

export type WriteArgs = {
  set?: { key: K; value: V }[];
  delete?: K[];
};

export type ListArgs = {
  gt?: K;
  gte?: K;
  lt?: K;
  lte?: K;
  limit?: number;
  offset?: number;
  reverse?: boolean;
};

export type OrderedKeyValueApi = {
  get(key: K): V | undefined;
  list(args?: ListArgs): { key: K; value: V }[];
  set(key: K, value: V): void;
  delete(key: K): void;
  write(tx: WriteArgs): void;
};
🗂️ File Structure Suggestion
pgsql
Copy
Edit
/src
  ├── bptree/
  │    ├── BPlusTree.ts          # Main class
  │    ├── Node.ts               # Node structure (leaf or internal)
  │    ├── utils.ts              # Prefix helpers, key encoding, etc.
  │    └── types.ts              # Shared interfaces
  ├── adapters/
  │    └── InMemoryKV.ts         # In-memory key-value store wrapper
  └── tests/
       └── BPlusTree.test.ts     # Unit tests
✅ Tasks
 Implement BPlusTree.get(key)

 Implement BPlusTree.set(key, value)

 Implement BPlusTree.delete(key)

 Implement BPlusTree.list(range)

 Implement BPlusTree.write(tx) (batch set/delete)

 Support depth-prefixed keys:

"00" prefix → leaf nodes

"01" prefix → level-1 internal nodes

and so on

📌 Notes
Use 00:<key> as the key for all leaves (key/value entries)

Internal nodes store ranges of child keys and forward pointers

Store the root key in a special metadata entry, e.g. meta:root

📊 Phase 2: Aggregation and Interval Logic
🎯 Goal
Generalize internal nodes to store aggregates (e.g. count, sum), enabling fast queries like:

ts
Copy
Edit
count({ gte: 'a', lt: 'z' });     // → returns number of entries in range
sum({ gte: 'a', lt: 'm' });       // → returns sum over values in range
✅ Tasks
 Store number of children under each internal node (.count)

 Add .count(args?: ListArgs): number

 Add .aggregate(args, reducerFn, initValue) API

 Implement reducer propagation logic on insert/delete

 Write tests for aggregation correctness

💡 Generalization
You should support arbitrary aggregation functions, like:

ts
Copy
Edit
const sumReducer = (acc: number, val: string) => acc + parseInt(val, 10);
tree.aggregate({ gte: 'a' }, sumReducer, 0);
📏 Phase 2.5: Interval Tree Operations (Optional, but Related)
🎯 Goal
Enable fast queries over overlapping intervals, e.g.:

ts
Copy
Edit
tree.overlaps(['2020-01-01', '2020-01-10']);
✅ Tasks
 Represent keys as ranges (e.g. [start, end])

 Store min/max range bounds in internal nodes

 Implement .overlaps([start, end]) API

 Add test cases for overlapping range edge cases

🌲 Phase 3: Dynamic Splitting with Rolling Hash (Merkle-Style)
🎯 Goal
Use content-defined chunking (e.g. hash-based boundary rules) to determine when to split nodes, just like Prolly Trees or Okra.

✅ Tasks
 Implement rolling hash logic

 Define boundary condition:
hash(key) % Q === 0 → split node

 Replace current split-on-size with split-on-hash

 Ensure deterministic chunking for syncing/mirroring later

 Keep stable root hash using anchor nodes

🧪 Testing
Write comprehensive tests for:

 Basic get/set/delete/list

 Node splits/merges

 Aggregation accuracy (count/sum)

 Interval overlaps

 Tree structure and key order

 Leaf-only scans (using "00" prefix filter)

 Edge cases: empty tree, exact limits, deletions

Use mocha or vitest, depending on the base project. Mock backends using InMemoryKV.

📚 References
Okra.js blog post

B+ Trees on Wikipedia

Interval Tree explanation

Dolt prolly tree blog

Rolling hash wiki

👷 Future Work Ideas
Implement sync() using Merkle tree traversal

Add tombstone support for deletions

Plug into IndexedDB or LMDB backends

Add CLI for tree visualization

Package into SQLite plugin

🙌 Contributing
This project is open source and modular. The goal is to build a fast, minimal, pluggable tree-backed key-value abstraction for local-first and decentralized apps.

yaml
Copy
Edit

---
