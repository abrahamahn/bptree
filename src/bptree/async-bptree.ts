import { AsyncOrderedKeyValueApi } from "../lib/types"

// Constants for tree structure
const LEAF_PREFIX = "00"
const INTERNAL_PREFIX = "01"
const METADATA_KEY = "__BPTREE_METADATA__"
const MAX_LEAF_SIZE = 32
const MAX_INTERNAL_SIZE = 32

// Types for tree nodes
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

export class AsyncBPlusTree implements AsyncOrderedKeyValueApi<string, string> {
    private rootId: string // Reference to root node
    private height: number = 0
    private initialized: boolean = false

    constructor(
        private store: AsyncOrderedKeyValueApi<string, string>,
        private maxLeafSize: number = MAX_LEAF_SIZE,
        private maxInternalSize: number = MAX_INTERNAL_SIZE
    ) {
        // Initialization is handled in the init method
    }

    async init(): Promise<void> {
        // Try to load existing metadata
        const metadataStr = await this.store.get(METADATA_KEY)
        
        if (metadataStr) {
            // Tree already exists, load metadata
            const metadata = JSON.parse(metadataStr) as TreeMetadata
            this.rootId = metadata.rootId
            this.height = metadata.height
        } else {
            // Initialize a new tree
            const rootNode: LeafNode = { keys: [], values: [] }
            this.rootId = LEAF_PREFIX + "root"
            this.height = 0
            
            await this.store.write({ 
                set: [
                    { key: this.rootId, value: JSON.stringify(rootNode) },
                    { key: METADATA_KEY, value: JSON.stringify({ rootId: this.rootId, height: this.height }) }
                ] 
            })
        }
        
        this.initialized = true
    }

    private async saveMetadata(): Promise<void> {
        const metadata: TreeMetadata = {
            rootId: this.rootId,
            height: this.height
        }
        
        await this.store.write({
            set: [{ key: METADATA_KEY, value: JSON.stringify(metadata) }]
        })
    }

    async get(key: string): Promise<string | undefined> {
        if (!this.initialized) await this.init()
        
        const leafNode = await this.findLeaf(key)
        const node = JSON.parse(await this.store.get(leafNode) || "{}") as LeafNode
        const index = node.keys.indexOf(key)
        return index !== -1 ? node.values[index] : undefined
    }

    async list(args: {
        gt?: string
        gte?: string
        lt?: string
        lte?: string
        limit?: number
        offset?: number
        reverse?: boolean
    } = {}): Promise<{ key: string; value: string }[]> {
        if (!this.initialized) await this.init()
        
        const results: { key: string; value: string }[] = []
        let current = await this.findLeaf(args.gte || args.gt || "")
        
        while (current && current !== "") {
            const node = JSON.parse(await this.store.get(current) || "{}") as LeafNode
            const startIndex = args.gt ? node.keys.findIndex(k => k > args.gt!) : 
                              args.gte ? node.keys.findIndex(k => k >= args.gte!) : 0
            
            for (let i = startIndex; i < node.keys.length; i++) {
                const key = node.keys[i]
                if (args.lt && key >= args.lt) return results
                if (args.lte && key > args.lte) return results
                
                results.push({ key, value: node.values[i] })
                if (args.limit && results.length >= args.limit) return results
            }
            
            current = node.next || ""
        }
        
        return results
    }

    async set(key: string, value: string): Promise<void> {
        if (!this.initialized) await this.init()
        await this.write({ set: [{ key, value }] })
    }

    async delete(key: string): Promise<void> {
        if (!this.initialized) await this.init()
        await this.write({ delete: [key] })
    }

    async write(tx: { set?: { key: string; value: string }[]; delete?: string[] }): Promise<void> {
        if (!this.initialized) await this.init()
        
        // Process each operation individually
        for (const { key, value } of tx.set || []) {
            await this.insert(key, value)
        }
        for (const key of tx.delete || []) {
            await this.remove(key)
        }
    }

    private async insert(key: string, value: string): Promise<void> {
        if (this.height === 0) {
            // Tree is empty or has only a root leaf node
            const node = JSON.parse(await this.store.get(this.rootId) || "{}") as LeafNode
            
            // Insert into leaf node
            const index = this.findInsertIndex(node.keys, key)
            
            // Replace value if key already exists
            if (index < node.keys.length && node.keys[index] === key) {
                node.values[index] = value
                await this.store.write({ set: [{ key: this.rootId, value: JSON.stringify(node) }] })
                return
            }
            
            node.keys.splice(index, 0, key)
            node.values.splice(index, 0, value)
            
            if (node.keys.length > this.maxLeafSize) {
                await this.splitLeafNode(this.rootId, node)
            } else {
                await this.store.write({ set: [{ key: this.rootId, value: JSON.stringify(node) }] })
            }
            return
        }
        
        // Find the leaf node where the key should be inserted
        const path = await this.findNodePath(key)
        const leafId = path[path.length - 1].id
        const leafNode = JSON.parse(await this.store.get(leafId) || "{}") as LeafNode
        
        // Insert into leaf node
        const index = this.findInsertIndex(leafNode.keys, key)
        
        // Replace value if key already exists
        if (index < leafNode.keys.length && leafNode.keys[index] === key) {
            leafNode.values[index] = value
            await this.store.write({ set: [{ key: leafId, value: JSON.stringify(leafNode) }] })
            return
        }
        
        leafNode.keys.splice(index, 0, key)
        leafNode.values.splice(index, 0, value)
        
        if (leafNode.keys.length > this.maxLeafSize) {
            await this.splitLeafNode(leafId, leafNode, path)
        } else {
            await this.store.write({ set: [{ key: leafId, value: JSON.stringify(leafNode) }] })
        }
    }

    private async remove(key: string): Promise<void> {
        if (this.height === 0) {
            const rootNode = JSON.parse(await this.store.get(this.rootId) || "{}") as LeafNode
            const index = rootNode.keys.indexOf(key)
            
            if (index === -1) return
            
            rootNode.keys.splice(index, 1)
            rootNode.values.splice(index, 1)
            await this.store.write({ set: [{ key: this.rootId, value: JSON.stringify(rootNode) }] })
            return
        }
        
        // Find the leaf node containing the key
        const path = await this.findNodePath(key)
        const leafId = path[path.length - 1].id
        const leafNode = JSON.parse(await this.store.get(leafId) || "{}") as LeafNode
        const index = leafNode.keys.indexOf(key)
        
        if (index === -1) return
        
        leafNode.keys.splice(index, 1)
        leafNode.values.splice(index, 1)
        
        if (leafNode.keys.length < Math.ceil(this.maxLeafSize / 2) && path.length > 1) {
            await this.rebalanceAfterDelete(leafId, leafNode, path)
        } else {
            await this.store.write({ set: [{ key: leafId, value: JSON.stringify(leafNode) }] })
        }
    }

    private async splitLeafNode(nodeId: string, node: LeafNode, path: NodePath[] = []): Promise<void> {
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
        
        await this.store.write({
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
            await this.store.write({ set: [{ key: newRootId, value: JSON.stringify(newRoot) }] })
            this.rootId = newRootId
            this.height = 1
            
            // Update metadata with new root and height
            await this.saveMetadata()
        } else {
            // Update parent node
            await this.updateParentAfterSplit(nodeId, newNodeId, splitKey, path)
        }
    }

    private async splitInternalNode(nodeId: string, node: InternalNode, path: NodePath[]): Promise<void> {
        const mid = Math.floor(node.keys.length / 2)
        const splitKey = node.keys[mid]
        const newNodeId = INTERNAL_PREFIX + Date.now() + Math.random().toString(36).substring(2, 10)
        
        const newNode: InternalNode = {
            keys: node.keys.slice(mid + 1),
            children: node.children.slice(mid + 1)
        }
        
        node.keys = node.keys.slice(0, mid)
        node.children = node.children.slice(0, mid + 1)
        
        await this.store.write({
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
            await this.store.write({ set: [{ key: newRootId, value: JSON.stringify(newRoot) }] })
            this.rootId = newRootId
            this.height++
            
            // Update metadata with new root and height
            await this.saveMetadata()
        } else {
            // Update parent
            const parentPath = path.slice(0, -1)
            const parentId = parentPath[parentPath.length - 1]?.id || this.rootId
            const parent = JSON.parse(await this.store.get(parentId) || "{}") as InternalNode
            const childIndex = path[path.length - 1].index
            
            // Insert the new node in the parent
            parent.keys.splice(childIndex, 0, splitKey)
            parent.children.splice(childIndex + 1, 0, newNodeId)
            
            if (parent.keys.length > this.maxInternalSize) {
                await this.splitInternalNode(parentId, parent, parentPath)
            } else {
                await this.store.write({ set: [{ key: parentId, value: JSON.stringify(parent) }] })
            }
        }
    }

    private async updateParentAfterSplit(leftId: string, rightId: string, splitKey: string, path: NodePath[]): Promise<void> {
        const parentPath = path.slice(0, -1)
        const parentId = parentPath[parentPath.length - 1]?.id || this.rootId
        const parent = JSON.parse(await this.store.get(parentId) || "{}") as InternalNode
        const childIndex = path[path.length - 1].index
        
        // Insert the new node in the parent
        parent.keys.splice(childIndex, 0, splitKey)
        parent.children.splice(childIndex + 1, 0, rightId)
        
        if (parent.keys.length > this.maxInternalSize) {
            await this.splitInternalNode(parentId, parent, parentPath)
        } else {
            await this.store.write({ set: [{ key: parentId, value: JSON.stringify(parent) }] })
        }
    }

    private async rebalanceAfterDelete(nodeId: string, node: LeafNode | InternalNode, path: NodePath[]): Promise<void> {
        const parentPath = path.slice(0, -1)
        const parentId = parentPath[parentPath.length - 1]?.id || this.rootId
        const parent = JSON.parse(await this.store.get(parentId) || "{}") as InternalNode
        const childIndex = path[path.length - 1].index
        
        // Try to borrow from left sibling
        if (childIndex > 0) {
            const leftSiblingId = parent.children[childIndex - 1]
            const leftSibling = JSON.parse(await this.store.get(leftSiblingId) || "{}") as any
            
            if (leftSibling.keys.length > Math.ceil(this.maxLeafSize / 2)) {
                await this.borrowFromLeftSibling(nodeId, node, leftSiblingId, leftSibling, parentId, parent, childIndex)
                return
            }
        }
        
        // Try to borrow from right sibling
        if (childIndex < parent.children.length - 1) {
            const rightSiblingId = parent.children[childIndex + 1]
            const rightSibling = JSON.parse(await this.store.get(rightSiblingId) || "{}") as any
            
            if (rightSibling.keys.length > Math.ceil(this.maxLeafSize / 2)) {
                await this.borrowFromRightSibling(nodeId, node, rightSiblingId, rightSibling, parentId, parent, childIndex)
                return
            }
        }
        
        // Merge with a sibling
        if (childIndex > 0) {
            // Merge with left sibling
            const leftSiblingId = parent.children[childIndex - 1]
            const leftSibling = JSON.parse(await this.store.get(leftSiblingId) || "{}") as any
            await this.mergeWithLeftSibling(nodeId, node, leftSiblingId, leftSibling, parentId, parent, childIndex, parentPath)
        } else {
            // Merge with right sibling
            const rightSiblingId = parent.children[childIndex + 1]
            const rightSibling = JSON.parse(await this.store.get(rightSiblingId) || "{}") as any
            await this.mergeWithRightSibling(nodeId, node, rightSiblingId, rightSibling, parentId, parent, childIndex, parentPath)
        }
    }

    private async borrowFromLeftSibling(
        nodeId: string,
        node: any,
        leftSiblingId: string,
        leftSibling: any,
        parentId: string,
        parent: InternalNode,
        childIndex: number
    ): Promise<void> {
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
        
        await this.store.write({
            set: [
                { key: nodeId, value: JSON.stringify(node) },
                { key: leftSiblingId, value: JSON.stringify(leftSibling) },
                { key: parentId, value: JSON.stringify(parent) }
            ]
        })
    }

    private async borrowFromRightSibling(
        nodeId: string,
        node: any,
        rightSiblingId: string,
        rightSibling: any,
        parentId: string,
        parent: InternalNode,
        childIndex: number
    ): Promise<void> {
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
        
        await this.store.write({
            set: [
                { key: nodeId, value: JSON.stringify(node) },
                { key: rightSiblingId, value: JSON.stringify(rightSibling) },
                { key: parentId, value: JSON.stringify(parent) }
            ]
        })
    }

    private async mergeWithLeftSibling(
        nodeId: string,
        node: any,
        leftSiblingId: string,
        leftSibling: any,
        parentId: string,
        parent: InternalNode,
        childIndex: number,
        parentPath: NodePath[]
    ): Promise<void> {
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
        
        await this.store.write({
            set: [
                { key: leftSiblingId, value: JSON.stringify(leftSibling) },
                { key: parentId, value: JSON.stringify(parent) }
            ],
            delete: [nodeId]
        })
        
        // Check if parent needs rebalancing
        if (parent.keys.length < Math.ceil(this.maxInternalSize / 2) && parentPath.length > 0) {
            await this.rebalanceAfterDelete(parentId, parent, parentPath)
        } else if (parent.keys.length === 0 && parentId === this.rootId) {
            // Root is empty, make the merged node the new root
            this.rootId = leftSiblingId
            this.height--
            await this.store.write({ delete: [parentId] })
            
            // Update metadata with new root and height
            await this.saveMetadata()
        }
    }

    private async mergeWithRightSibling(
        nodeId: string,
        node: any,
        rightSiblingId: string,
        rightSibling: any,
        parentId: string,
        parent: InternalNode,
        childIndex: number,
        parentPath: NodePath[]
    ): Promise<void> {
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
        
        await this.store.write({
            set: [
                { key: nodeId, value: JSON.stringify(node) },
                { key: parentId, value: JSON.stringify(parent) }
            ],
            delete: [rightSiblingId]
        })
        
        // Check if parent needs rebalancing
        if (parent.keys.length < Math.ceil(this.maxInternalSize / 2) && parentPath.length > 0) {
            await this.rebalanceAfterDelete(parentId, parent, parentPath)
        } else if (parent.keys.length === 0 && parentId === this.rootId) {
            // Root is empty, make the merged node the new root
            this.rootId = nodeId
            this.height--
            await this.store.write({ delete: [parentId] })
            
            // Update metadata with new root and height
            await this.saveMetadata()
        }
    }

    private async findNodePath(key: string): Promise<NodePath[]> {
        const path: NodePath[] = []
        let current = this.rootId
        let level = 0
        
        while (level < this.height) {
            const node = JSON.parse(await this.store.get(current) || "{}") as InternalNode
            const index = this.findChildIndex(node.keys, key)
            path.push({ id: current, index })
            current = node.children[index] || node.children[node.children.length - 1]
            level++
        }
        
        path.push({ id: current, index: -1 }) // Leaf node doesn't need an index
        return path
    }

    private async findLeaf(key: string): Promise<string> {
        let current = this.rootId
        let level = 0
        
        while (level < this.height) {
            const node = JSON.parse(await this.store.get(current) || "{}") as InternalNode
            const index = this.findChildIndex(node.keys, key)
            current = node.children[index] || node.children[node.children.length - 1]
            level++
        }
        
        return current
    }

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