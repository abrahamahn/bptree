# B+ Tree Implementation Project

A TypeScript project to build an efficient B+ tree implementation on top of an ordered key-value store, inspired by [Merklizing the key-value store](https://joelgustafson.com/posts/2023-05-04/merklizing-the-key-value-store-for-fun-and-profit).

## Project Goals

This project aims to implement a B+ tree data structure that works with various storage backends while maintaining a consistent API interface. The development is planned in three phases:

### Phase 1: B+ Tree with Ordered Key-Value Store Interface

Build a B+ tree implementation that maintains the following interface:

```typescript
type K = string
type V = string

export type WriteArgs = { 
  set?: { key: K; value: V }[]; 
  delete?: K[] 
}

export type ListArgs = {
  gt?: K
  gte?: K
  lt?: K
  lte?: K
  limit?: number
  offset?: number
  reverse?: boolean
}

export type OrderedKeyValueApi = {
  get: (key: K) => V | undefined
  list(args?: ListArgs<K>): { key: K; value: V }[]
  set: (key: K, value: V) => void
  delete: (key: K) => void
  write: (tx: WriteArgs<K, V>) => void
}
```

Key requirements:
- Work with synchronous storage (InMemoryDatabase)
- Work with asynchronous storage (IndexedDB) using native transaction capabilities
- Work with LMDB (synchronous read, asynchronous write)
- Use a depth-prefixed key approach (similar to Okra.js) where nodes are stored with a prefix indicating the layer of the tree

### Phase 2: Aggregation and Range Operations

Extend the B+ tree to:
- Store the number of leaf nodes underneath each node
- Support efficient count operations for ranges
- Implement a generalized reducer for aggregating data up the tree
- Build an interval tree for efficient overlap queries

### Phase 3: Advanced Tree Operations

- Implement intelligent node splitting/merging logic
- Add Merkle tree (Prolly Tree) functionality using rolling hash functions
- Consider Dolt's approach for more consistent chunk sizes

## Getting Started

### Prerequisites

- Node.js (v14+)
- npm or yarn

### Setup

```bash
git clone https://github.com/abrahamahn/bptree.git
cd bptree
npm install
```

### Testing

```bash
# Run all tests
npm test
```

### Running Benchmarks

```bash
# Run performance benchmarks
npm run benchmark
```

### Current Status

- [x] Project setup based on typescript-library template
- [ ] Phase 1: Basic B+ tree implementation
- [ ] Phase 2: Aggregation functionality
- [ ] Phase 3: Advanced tree operations