
/**
 * Basic key-value write transaction
 */
export type WriteArgs<K = string, V = any> = { 
    set?: { key: K; value: V }[]; 
    delete?: K[] 
  }

/**
 * Parameters for listing/range queries
 */
export type ListArgs<K = string> = {
gt?: K
gte?: K
lt?: K
lte?: K
limit?: number
offset?: number
reverse?: boolean
}

/**
 * Base Key-Value API interface
 */
export interface KeyValueApi<K = string, V = any> {
get(key: K): V | undefined
write(tx: WriteArgs<K, V>): void
}

/**
 * Asynchronous version of KeyValueApi
 */
export interface AsyncKeyValueApi<K = string, V = any> {
get(key: K): Promise<V | undefined>
write(tx: WriteArgs<K, V>): Promise<void>
}

/**
 * Ordered Key-Value API interface with range queries
 */
export interface OrderedKeyValueApi<K = string, V = any> extends KeyValueApi<K, V> {
list(args?: ListArgs<K>): { key: K; value: V }[]
set(key: K, value: V): void
delete(key: K): void
}

/**
 * Asynchronous version of OrderedKeyValueApi
 */
export interface AsyncOrderedKeyValueApi<K = string, V = any> {
get(key: K): Promise<V | undefined>
list(args?: ListArgs<K>): Promise<{ key: K; value: V }[]>
set(key: K, value: V): Promise<void>
delete(key: K): Promise<void>
write(tx: WriteArgs<K, V>): Promise<void>
}
