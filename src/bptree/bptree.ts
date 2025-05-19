import { OrderedKeyValueApi, WriteArgs, ListArgs } from '../lib/types'

const LEAF_PREFIX = "00"
const INTERNAL_PREFIX = "01"
const METADATA_KEY = "__BPTREE_METADATA__"
const DEFAULT_MAX_LEAF_SIZE = 32
const DEFAULT_MAX_INTERNAL_SIZE = 32

type LeafNode = {
    keys: string[]
    values: string[]
    next?: string // Reference to next leaf node for range scans
}

type InternalNode = {
    keys: string[]
    children: string[] // References to child nodes
}

type NodePath = {
    id: string
    index: number
}

type TreeMetadata = {
    rootId: string
    height: number
}

/**
 * A B+ Tree implementation on top of an ordered key-value store.
 * This implementation uses a prefix-based approach for storing nodes:
 * - Leaf nodes are prefixed with "00"
 * - Internal nodes are prefixed with "01"
 */
export class BPlusTree implements OrderedKeyValueApi<string, string> {
    private rootId: string // Reference to root node
    private height: number = 0
    private initialized: boolean = false

    /**
     * Creates a new B+ Tree
     * 
     * @param store The underlying store to use
     * @param maxLeafSize Maximum number of keys in a leaf node before splitting
     * @param maxInternalSize Maximum number of keys in an internal node before splitting
     */
    constructor(
        private store: OrderedKeyValueApi<string, string>,
        private maxLeafSize: number = DEFAULT_MAX_LEAF_SIZE,
        private maxInternalSize: number = DEFAULT_MAX_INTERNAL_SIZE
    ) {
        this.init()
    }

    /**
     * Initializes the B+ Tree, either loading an existing tree or creating a new one
     */
    private init(): void {
        // Try to load existing metadata
        const metadataStr = this.store.get(METADATA_KEY)
        
        if (metadataStr) {
            // Tree already exists, load metadata
            const metadata = JSON.parse(metadataStr) as TreeMetadata
            this.rootId = metadata.rootId
            this.height = metadata.height
        } else {
            // Initialize a new tree with an empty root leaf node
            const rootNode: LeafNode = { keys: [], values: [] }
            this.rootId = LEAF_PREFIX + "root"
            this.height = 0
            
            this.store.write({ 
                set: [
                    { key: this.rootId, value: JSON.stringify(rootNode) },
                    { key: METADATA_KEY, value: JSON.stringify({ rootId: this.rootId, height: this.height }) }
                ] 
            })
        }
        
        this.initialized = true
    }

    /**
     * Saves the current tree metadata to the store
     */
    private saveMetadata(): void {
        const metadata: TreeMetadata = {
            rootId: this.rootId,
            height: this.height
        }
        
        this.store.write({
            set: [{ key: METADATA_KEY, value: JSON.stringify(metadata) }]
        })
    }

    /**
     * Gets a value by key
     * 
     * @param key The key to look up
     * @returns The value or undefined if not found
     */
    get(key: string): string | undefined {
        if (!this.initialized) this.init()
        
        const leafNode = this.findLeaf(key)
        const node = JSON.parse(this.store.get(leafNode) || "{}") as LeafNode
        const index = node.keys.indexOf(key)
        return index !== -1 ? node.values[index] : undefined
    }

    /**
     * Lists entries in a range
     * 
     * @param args Range query parameters
     * @returns Array of key-value pairs in the range
     */
    list(args: ListArgs<string> = {}): { key: string; value: string }[] {
        if (!this.initialized) this.init()
        
        const results: { key: string; value: string }[] = []
        let current = this.findLeaf(args.gte || args.gt || "")
        
        while (current && current !== "") {
            const node = JSON.parse(this.store.get(current) || "{}") as LeafNode
            const startIndex = args.gt ? node.keys.findIndex(k => k > args.gt!) : 
                              args.gte ? node.keys.findIndex(k => k >= args.gte!) : 0
            
            if (startIndex === -1) {
                // No keys in this node match our criteria, move to next node
                current = node.next || ""
                continue
            }
            
            for (let i = startIndex; i < node.keys.length; i++) {
                const key = node.keys[i]
                if (args.lt && key >= args.lt) return results
                if (args.lte && key > args.lte) return results
                
                results.push({ key, value: node.values[i] })
                if (args.limit && results.length >= args.limit) return results
            }
            
            current = node.next || ""
        }
        
        // Apply offset and reverse if needed
        if (args.offset && args.offset > 0) {
            results.splice(0, args.offset)
        }
        
        if (args.reverse) {
            results.reverse()
        }
        
        return results
    }

    /**
     * Sets a value for a key
     * 
     * @param key The key to set
     * @param value The value to set
     */
    set(key: string, value: string): void {
        if (!this.initialized) this.init()
        this.write({ set: [{ key, value }] })
    }

    /**
     * Deletes a key
     * 
     * @param key The key to delete
     */
    delete(key: string): void {
        if (!this.initialized) this.init()
        this.write({ delete: [key] })
    }

    /**
     * Performs a batch write operation
     * 
     * @param tx The write transaction
     */
    write(tx: WriteArgs<string, string>): void {
        if (!this.initialized) this.init()
        
        // Process each operation individually
        for (const { key, value } of tx.set || []) {
            this.insert(key, value)
        }
        for (const key of tx.delete || []) {
            this.remove(key)
        }
    }

    /**
     * Inserts a key-value pair into the tree
     * 
     * @param key The key to insert
     * @param value The value to insert
     */
    private insert(key: string, value: string): void {
        if (this.height === 0) {
            // Tree is empty or has only a root leaf node
            const node = JSON.parse(this.store.get(this.rootId) || "{}") as LeafNode
            
            // Insert into leaf node
            const index = this.findInsertIndex(node.keys, key)
            
            // Replace value if key already exists
            if (index < node.keys.length && node.keys[index] === key) {
                node.values[index] = value
                this.store.write({ set: [{ key: this.rootId, value: JSON.stringify(node) }] })
                return
            }
            
            node.keys.splice(index, 0, key)
            node.values.splice(index, 0, value)
            
            if (node.keys.length > this.maxLeafSize) {
                this.splitLeafNode(this.rootId, node)
            } else {
                this.store.write({ set: [{ key: this.rootId, value: JSON.stringify(node) }] })
            }
            return
        }
        
        // Find the leaf node where the key should be inserted
        const path = this.findNodePath(key)
        const leafId = path[path.length - 1].id
        const leafNode = JSON.parse(this.store.get(leafId) || "{}") as LeafNode
        
        // Insert into leaf node
        const index = this.findInsertIndex(leafNode.keys, key)
        
        // Replace value if key already exists
        if (index < leafNode.keys.length && leafNode.keys[index] === key) {
            leafNode.values[index] = value
            this.store.write({ set: [{ key: leafId, value: JSON.stringify(leafNode) }] })
            return
        }
        
        leafNode.keys.splice(index, 0, key)
        leafNode.values.splice(index, 0, value)
        
        if (leafNode.keys.length > this.maxLeafSize) {
            this.splitLeafNode(leafId, leafNode, path)
        } else {
            this.store.write({ set: [{ key: leafId, value: JSON.stringify(leafNode) }] })
        }
    }

    /**
     * Removes a key-value pair from the tree
     * 
     * @param key The key to remove
     */
    private remove(key: string): void {
        if (this.height === 0) {
            const rootNode = JSON.parse(this.store.get(this.rootId) || "{}") as LeafNode
            const index = rootNode.keys.indexOf(key)
            
            if (index === -1) return
            
            rootNode.keys.splice(index, 1)
            rootNode.values.splice(index, 1)
            this.store.write({ set: [{ key: this.rootId, value: JSON.stringify(rootNode) }] })
            return
        }
        
        // Find the leaf node containing the key
        const path = this.findNodePath(key)
        const leafId = path[path.length - 1].id
        const leafNode = JSON.parse(this.store.get(leafId) || "{}") as LeafNode
        const index = leafNode.keys.indexOf(key)
        
        if (index === -1) return
        
        leafNode.keys.splice(index, 1)
        leafNode.values.splice(index, 1)
        
        if (leafNode.keys.length < Math.ceil(this.maxLeafSize / 2) && path.length > 1) {
            this.rebalanceAfterDelete(leafId, leafNode, path)
        } else {
            this.store.write({ set: [{ key: leafId, value: JSON.stringify(leafNode) }] })
        }
    }

    /**
     * Splits a leaf node when it exceeds the maximum size
     * 
     * @param nodeId ID of the node to split
     * @param node The node to split
     * @param path Path to the node (optional)
     */
    private splitLeafNode(nodeId: string, node: LeafNode, path: NodePath[] = []): void {
        const mid = Math.floor(node.keys.length / 2)
        const newNodeId = LEAF_PREFIX + Date.now() + Math.random().toString(36).substring(2, 10)
        
        const newNode: LeafNode = {
            keys: node.keys.slice(mid),
            values: node.values.slice(mid),
            next: node.next
        }
        
        node.keys = node.keys.slice(0, mid)
        node.values = node.values.slice(0, mid)
        node.next = newNodeId
        
        this.store.write({
            set: [
                { key: nodeId, value: JSON.stringify(node) },
                { key: newNodeId, value: JSON.stringify(newNode) }
            ]
        })
        
        // Update parent or create new root
        const splitKey = newNode.keys[0]
        if (path.length === 0) {
            // This is the root node, create a new root
            const newRoot: InternalNode = {
                keys: [splitKey],
                children: [nodeId, newNodeId]
            }
            const newRootId = INTERNAL_PREFIX + Date.now() + Math.random().toString(36).substring(2, 10)
            this.store.write({ set: [{ key: newRootId, value: JSON.stringify(newRoot) }] })
            this.rootId = newRootId
            this.height = 1
            
            // Update metadata with new root and height
            this.saveMetadata()
        } else {
            // Update parent node
            this.updateParentAfterSplit(nodeId, newNodeId, splitKey, path)
        }
    }

    /**
     * Splits an internal node when it exceeds the maximum size
     * 
     * @param nodeId ID of the node to split
     * @param node The node to split
     * @param path Path to the node
     */
    private splitInternalNode(nodeId: string, node: InternalNode, path: NodePath[]): void {
        const mid = Math.floor(node.keys.length / 2)
        const splitKey = node.keys[mid]
        const newNodeId = INTERNAL_PREFIX + Date.now() + Math.random().toString(36).substring(2, 10)
        
        const newNode: InternalNode = {
            keys: node.keys.slice(mid + 1),
            children: node.children.slice(mid + 1)
        }
        
        node.keys = node.keys.slice(0, mid)
        node.children = node.children.slice(0, mid + 1)
        
        this.store.write({
            set: [
                { key: nodeId, value: JSON.stringify(node) },
                { key: newNodeId, value: JSON.stringify(newNode) }
            ]
        })
        
        if (path.length === 0) {
            // This is the root node, create a new root
            const newRoot: InternalNode = {
                keys: [splitKey],
                children: [nodeId, newNodeId]
            }
            const newRootId = INTERNAL_PREFIX + Date.now() + Math.random().toString(36).substring(2, 10)
            this.store.write({ set: [{ key: newRootId, value: JSON.stringify(newRoot) }] })
            this.rootId = newRootId
            this.height++
            
            // Update metadata with new root and height
            this.saveMetadata()
        } else {
            // Update parent
            const parentPath = path.slice(0, -1)
            const parentId = parentPath[parentPath.length - 1]?.id || this.rootId
            const parent = JSON.parse(this.store.get(parentId) || "{}") as InternalNode
            const childIndex = path[path.length - 1].index
            
            // Insert the new node in the parent
            parent.keys.splice(childIndex, 0, splitKey)
            parent.children.splice(childIndex + 1, 0, newNodeId)
            
            if (parent.keys.length > this.maxInternalSize) {
                this.splitInternalNode(parentId, parent, parentPath)
            } else {
                this.store.write({ set: [{ key: parentId, value: JSON.stringify(parent) }] })
            }
        }
    }

    /**
     * Updates a parent node after a child split
     * 
     * @param leftId ID of the left child
     * @param rightId ID of the right child
     * @param splitKey The key that separates the children
     * @param path Path to the node
     */
    private updateParentAfterSplit(leftId: string, rightId: string, splitKey: string, path: NodePath[]): void {
        const parentPath = path.slice(0, -1)
        const parentId = parentPath[parentPath.length - 1]?.id || this.rootId
        const parent = JSON.parse(this.store.get(parentId) || "{}") as InternalNode
        const childIndex = path[path.length - 1].index
        
        // Insert the new node in the parent
        parent.keys.splice(childIndex, 0, splitKey)
        parent.children.splice(childIndex + 1, 0, rightId)
        
        if (parent.keys.length > this.maxInternalSize) {
            this.splitInternalNode(parentId, parent, parentPath)
        } else {
            this.store.write({ set: [{ key: parentId, value: JSON.stringify(parent) }] })
        }
    }

    /**
     * Rebalances the tree after a delete operation
     * 
     * @param nodeId ID of the node to rebalance
     * @param node The node to rebalance
     * @param path Path to the node
     */
    private rebalanceAfterDelete(nodeId: string, node: LeafNode | InternalNode, path: NodePath[]): void {
        const parentPath = path.slice(0, -1)
        const parentId = parentPath[parentPath.length - 1]?.id || this.rootId
        const parent = JSON.parse(this.store.get(parentId) || "{}") as InternalNode
        const childIndex = path[path.length - 1].index
        
        // Try to borrow from left sibling
        if (childIndex > 0) {
            const leftSiblingId = parent.children[childIndex - 1]
            const leftSibling = JSON.parse(this.store.get(leftSiblingId) || "{}") as any
            
            if (leftSibling.keys.length > Math.ceil(this.maxLeafSize / 2)) {
                this.borrowFromLeftSibling(nodeId, node, leftSiblingId, leftSibling, parentId, parent, childIndex)
                return
            }
        }
        
        // Try to borrow from right sibling
        if (childIndex < parent.children.length - 1) {
            const rightSiblingId = parent.children[childIndex + 1]
            const rightSibling = JSON.parse(this.store.get(rightSiblingId) || "{}") as any
            
            if (rightSibling.keys.length > Math.ceil(this.maxLeafSize / 2)) {
                this.borrowFromRightSibling(nodeId, node, rightSiblingId, rightSibling, parentId, parent, childIndex)
                return
            }
        }
        
        // Merge with a sibling
        if (childIndex > 0) {
            // Merge with left sibling
            const leftSiblingId = parent.children[childIndex - 1]
            const leftSibling = JSON.parse(this.store.get(leftSiblingId) || "{}") as any
            this.mergeWithLeftSibling(nodeId, node, leftSiblingId, leftSibling, parentId, parent, childIndex, parentPath)
        } else {
            // Merge with right sibling
            const rightSiblingId = parent.children[childIndex + 1]
            const rightSibling = JSON.parse(this.store.get(rightSiblingId) || "{}") as any
            this.mergeWithRightSibling(nodeId, node, rightSiblingId, rightSibling, parentId, parent, childIndex, parentPath)
        }
    }

    /**
     * Borrows a key-value pair from the left sibling
     */
    private borrowFromLeftSibling(
        nodeId: string,
        node: any,
        leftSiblingId: string,
        leftSibling: any,
        parentId: string,
        parent: InternalNode,
        childIndex: number
    ): void {
        // For leaf nodes
        if ('values' in node && 'values' in leftSibling) {
            // Move the rightmost key-value pair from left sibling to node
            const key = leftSibling.keys.pop()
            const value = leftSibling.values.pop()
            node.keys.unshift(key)
            node.values.unshift(value)
            
            // Update parent key
            parent.keys[childIndex - 1] = node.keys[0]
        } else {
            // For internal nodes
            // Move the rightmost child from left sibling to node
            const parentKey = parent.keys[childIndex - 1]
            const childId = leftSibling.children.pop()
            const key = leftSibling.keys.pop()
            
            node.keys.unshift(parentKey)
            node.children.unshift(childId)
            parent.keys[childIndex - 1] = key
        }
        
        this.store.write({
            set: [
                { key: nodeId, value: JSON.stringify(node) },
                { key: leftSiblingId, value: JSON.stringify(leftSibling) },
                { key: parentId, value: JSON.stringify(parent) }
            ]
        })
    }

    /**
     * Borrows a key-value pair from the right sibling
     */
    private borrowFromRightSibling(
        nodeId: string,
        node: any,
        rightSiblingId: string,
        rightSibling: any,
        parentId: string,
        parent: InternalNode,
        childIndex: number
    ): void {
        // For leaf nodes
        if ('values' in node && 'values' in rightSibling) {
            // Move the leftmost key-value pair from right sibling to node
            const key = rightSibling.keys.shift()
            const value = rightSibling.values.shift()
            node.keys.push(key)
            node.values.push(value)
            
            // Update parent key
            parent.keys[childIndex] = rightSibling.keys[0]
        } else {
            // For internal nodes
            // Move the leftmost child from right sibling to node
            const parentKey = parent.keys[childIndex]
            const childId = rightSibling.children.shift()
            const key = rightSibling.keys.shift()
            
            node.keys.push(parentKey)
            node.children.push(childId)
            parent.keys[childIndex] = key
        }
        
        this.store.write({
            set: [
                { key: nodeId, value: JSON.stringify(node) },
                { key: rightSiblingId, value: JSON.stringify(rightSibling) },
                { key: parentId, value: JSON.stringify(parent) }
            ]
        })
    }

    /**
     * Merges a node with its left sibling
     */
    private mergeWithLeftSibling(
        nodeId: string,
        node: any,
        leftSiblingId: string,
        leftSibling: any,
        parentId: string,
        parent: InternalNode,
        childIndex: number,
        parentPath: NodePath[]
    ): void {
        // For leaf nodes
        if ('values' in node && 'values' in leftSibling) {
            leftSibling.keys = [...leftSibling.keys, ...node.keys]
            leftSibling.values = [...leftSibling.values, ...node.values]
            leftSibling.next = node.next
        } else {
            // For internal nodes
            // Include the parent key in the merge
            const parentKey = parent.keys[childIndex - 1]
            leftSibling.keys = [...leftSibling.keys, parentKey, ...node.keys]
            leftSibling.children = [...leftSibling.children, ...node.children]
        }
        
        // Remove the node and update parent
        parent.keys.splice(childIndex - 1, 1)
        parent.children.splice(childIndex, 1)
        
        this.store.write({
            set: [
                { key: leftSiblingId, value: JSON.stringify(leftSibling) },
                { key: parentId, value: JSON.stringify(parent) }
            ],
            delete: [nodeId]
        })
        
        // Check if parent needs rebalancing
        if (parent.keys.length < Math.ceil(this.maxInternalSize / 2) && parentPath.length > 0) {
            this.rebalanceAfterDelete(parentId, parent, parentPath)
        } else if (parent.keys.length === 0 && parentId === this.rootId) {
            // Root is empty, make the merged node the new root
            this.rootId = leftSiblingId
            this.height--
            this.store.write({ delete: [parentId] })
            
            // Update metadata with new root and height
            this.saveMetadata()
        }
    }

    /**
     * Merges a node with its right sibling
     */
    private mergeWithRightSibling(
        nodeId: string,
        node: any,
        rightSiblingId: string,
        rightSibling: any,
        parentId: string,
        parent: InternalNode,
        childIndex: number,
        parentPath: NodePath[]
    ): void {
        // For leaf nodes
        if ('values' in node && 'values' in rightSibling) {
            node.keys = [...node.keys, ...rightSibling.keys]
            node.values = [...node.values, ...rightSibling.values]
            node.next = rightSibling.next
        } else {
            // For internal nodes
            // Include the parent key in the merge
            const parentKey = parent.keys[childIndex]
            node.keys = [...node.keys, parentKey, ...rightSibling.keys]
            node.children = [...node.children, ...rightSibling.children]
        }
        
        // Remove the right sibling and update parent
        parent.keys.splice(childIndex, 1)
        parent.children.splice(childIndex + 1, 1)
        
        this.store.write({
            set: [
                { key: nodeId, value: JSON.stringify(node) },
                { key: parentId, value: JSON.stringify(parent) }
            ],
            delete: [rightSiblingId]
        })
        
        // Check if parent needs rebalancing
        if (parent.keys.length < Math.ceil(this.maxInternalSize / 2) && parentPath.length > 0) {
            this.rebalanceAfterDelete(parentId, parent, parentPath)
        } else if (parent.keys.length === 0 && parentId === this.rootId) {
            // Root is empty, make the merged node the new root
            this.rootId = nodeId
            this.height--
            this.store.write({ delete: [parentId] })
            
            // Update metadata with new root and height
            this.saveMetadata()
        }
    }

    /**
     * Finds the path from the root to the leaf node that should contain the key
     * 
     * @param key The key to look up
     * @returns Array of node paths from root to leaf
     */
    private findNodePath(key: string): NodePath[] {
        const path: NodePath[] = []
        let current = this.rootId
        let level = 0
        
        while (level < this.height) {
            const node = JSON.parse(this.store.get(current) || "{}") as InternalNode
            const index = this.findChildIndex(node.keys, key)
            path.push({ id: current, index })
            current = node.children[index] || node.children[node.children.length - 1]
            level++
        }
        
        path.push({ id: current, index: -1 }) // Leaf node doesn't need an index
        return path
    }

    /**
     * Finds the leaf node that should contain the key
     * 
     * @param key The key to look up
     * @returns ID of the leaf node
     */
    private findLeaf(key: string): string {
        let current = this.rootId
        let level = 0
        
        while (level < this.height) {
            const node = JSON.parse(this.store.get(current) || "{}") as InternalNode
            const index = this.findChildIndex(node.keys, key)
            current = node.children[index] || node.children[node.children.length - 1]
            level++
        }
        
        return current
    }

    /**
     * Finds the index of the child that should contain the key
     * 
     * @param keys Array of keys
     * @param key The key to look up
     * @returns Index of the child
     */
    private findChildIndex(keys: string[], key: string): number {
        let left = 0
        let right = keys.length
        
        while (left < right) {
            const mid = Math.floor((left + right) / 2)
            if (keys[mid] <= key) {
                left = mid + 1
            } else {
                right = mid
            }
        }
        
        return left
    }

    /**
     * Finds the index where a key should be inserted
     * 
     * @param keys Array of keys
     * @param key The key to insert
     * @returns Index where the key should be inserted
     */
    private findInsertIndex(keys: string[], key: string): number {
        let left = 0
        let right = keys.length
        
        while (left < right) {
            const mid = Math.floor((left + right) / 2)
            if (keys[mid] < key) {
                left = mid + 1
            } else if (keys[mid] === key) {
                return mid
            } else {
                right = mid
            }
        }
        
        return left
    }
}
