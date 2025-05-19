import { OrderedKeyValueApi } from "../lib/types"

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

export class BPlusTree implements OrderedKeyValueApi<string, string> {
    private rootId: string // Reference to root node
    private height: number = 0
    private initialized: boolean = false

    constructor(
        private store: OrderedKeyValueApi<string, string>,
        private maxLeafSize: number = MAX_LEAF_SIZE,
        private maxInternalSize: number = MAX_INTERNAL_SIZE
    ) {
        this.init();
    }

    private init(): void {
        // Try to load existing metadata
        const metadataStr = this.store.get(METADATA_KEY);
        
        if (metadataStr) {
            // Tree already exists, load metadata
            const metadata = JSON.parse(metadataStr) as TreeMetadata;
            this.rootId = metadata.rootId;
            this.height = metadata.height;
        } else {
            // Initialize a new tree
            const rootNode: LeafNode = { keys: [], values: [] };
            this.rootId = LEAF_PREFIX + "root";
            this.height = 0;
            
            this.store.write({ 
                set: [
                    { key: this.rootId, value: JSON.stringify(rootNode) },
                    { key: METADATA_KEY, value: JSON.stringify({ rootId: this.rootId, height: this.height }) }
                ] 
            });
        }
        
        this.initialized = true;
    }

    private saveMetadata(): void {
        const metadata: TreeMetadata = {
            rootId: this.rootId,
            height: this.height
        };
        
        this.store.write({
            set: [{ key: METADATA_KEY, value: JSON.stringify(metadata) }]
        });
    }

    get(key: string): string | undefined {
        if (!this.initialized) this.init();
        
        const leafNode = this.findLeaf(key);
        const node = JSON.parse(this.store.get(leafNode) || "{}") as LeafNode;
        const index = node.keys.indexOf(key);
        return index !== -1 ? node.values[index] : undefined;
    }

    list(args: {
        gt?: string
        gte?: string
        lt?: string
        lte?: string
        limit?: number
        offset?: number
        reverse?: boolean
    } = {}): { key: string; value: string }[] {
        if (!this.initialized) this.init();
        
        const results: { key: string; value: string }[] = [];
        let current = this.findLeaf(args.gte || args.gt || "");
        
        while (current && current !== "") {
            const node = JSON.parse(this.store.get(current) || "{}") as LeafNode;
            const startIndex = args.gt ? node.keys.findIndex(k => k > args.gt!) : 
                              args.gte ? node.keys.findIndex(k => k >= args.gte!) : 0;
            
            for (let i = startIndex; i < node.keys.length; i++) {
                const key = node.keys[i];
                if (args.lt && key >= args.lt) return results;
                if (args.lte && key > args.lte) return results;
                
                results.push({ key, value: node.values[i] });
                if (args.limit && results.length >= args.limit) return results;
            }
            
            current = node.next || "";
        }
        
        return results;
    }

    set(key: string, value: string): void {
        if (!this.initialized) this.init();
        this.write({ set: [{ key, value }] });
    }

    delete(key: string): void {
        if (!this.initialized) this.init();
        this.write({ delete: [key] });
    }

    write(tx: { set?: { key: string; value: string }[]; delete?: string[] }): void {
        if (!this.initialized) this.init();
        
        for (const { key, value } of tx.set || []) {
            this.insert(key, value);
        }
        for (const key of tx.delete || []) {
            this.remove(key);
        }
    }

    private insert(key: string, value: string): void {
        if (this.height === 0) {
            // Tree is empty or has only a root leaf node
            const node = JSON.parse(this.store.get(this.rootId) || "{}") as LeafNode;
            
            // Insert into leaf node
            const index = this.findInsertIndex(node.keys, key);
            
            // Replace value if key already exists
            if (index < node.keys.length && node.keys[index] === key) {
                node.values[index] = value;
                this.store.write({ set: [{ key: this.rootId, value: JSON.stringify(node) }] });
                return;
            }
            
            node.keys.splice(index, 0, key);
            node.values.splice(index, 0, value);
            
            if (node.keys.length > this.maxLeafSize) {
                this.splitLeafNode(this.rootId, node);
            } else {
                this.store.write({ set: [{ key: this.rootId, value: JSON.stringify(node) }] });
            }
            return;
        }
        
        // Find the leaf node where the key should be inserted
        const path = this.findNodePath(key);
        const leafId = path[path.length - 1].id;
        const leafNode = JSON.parse(this.store.get(leafId) || "{}") as LeafNode;
        
        // Insert into leaf node
        const index = this.findInsertIndex(leafNode.keys, key);
        
        // Replace value if key already exists
        if (index < leafNode.keys.length && leafNode.keys[index] === key) {
            leafNode.values[index] = value;
            this.store.write({ set: [{ key: leafId, value: JSON.stringify(leafNode) }] });
            return;
        }
        
        leafNode.keys.splice(index, 0, key);
        leafNode.values.splice(index, 0, value);
        
        if (leafNode.keys.length > this.maxLeafSize) {
            this.splitLeafNode(leafId, leafNode, path);
        } else {
            this.store.write({ set: [{ key: leafId, value: JSON.stringify(leafNode) }] });
        }
    }

    private remove(key: string): void {
        if (this.height === 0) {
            const rootNode = JSON.parse(this.store.get(this.rootId) || "{}") as LeafNode;
            const index = rootNode.keys.indexOf(key);
            
            if (index === -1) return;
            
            rootNode.keys.splice(index, 1);
            rootNode.values.splice(index, 1);
            this.store.write({ set: [{ key: this.rootId, value: JSON.stringify(rootNode) }] });
            return;
        }
        
        // Find the leaf node containing the key
        const path = this.findNodePath(key);
        const leafId = path[path.length - 1].id;
        const leafNode = JSON.parse(this.store.get(leafId) || "{}") as LeafNode;
        const index = leafNode.keys.indexOf(key);
        
        if (index === -1) return;
        
        leafNode.keys.splice(index, 1);
        leafNode.values.splice(index, 1);
        
        if (leafNode.keys.length < Math.ceil(this.maxLeafSize / 2) && path.length > 1) {
            this.rebalanceAfterDelete(leafId, leafNode, path);
        } else {
            this.store.write({ set: [{ key: leafId, value: JSON.stringify(leafNode) }] });
        }
    }

    private splitLeafNode(nodeId: string, node: LeafNode, path: NodePath[] = []): void {
        const mid = Math.floor(node.keys.length / 2);
        const newNodeId = LEAF_PREFIX + Date.now() + Math.random().toString(36).substring(2, 10);
        
        const newNode: LeafNode = {
            keys: node.keys.slice(mid),
            values: node.values.slice(mid),
            next: node.next
        };
        
        node.keys = node.keys.slice(0, mid);
        node.values = node.values.slice(0, mid);
        node.next = newNodeId;
        
        this.store.write({
            set: [
                { key: nodeId, value: JSON.stringify(node) },
                { key: newNodeId, value: JSON.stringify(newNode) }
            ]
        });
        
        // Update parent or create new root
        const splitKey = newNode.keys[0];
        if (path.length === 0) {
            // This is the root node, create a new root
            const newRoot: InternalNode = {
                keys: [splitKey],
                children: [nodeId, newNodeId]
            };
            const newRootId = INTERNAL_PREFIX + Date.now() + Math.random().toString(36).substring(2, 10);
            this.store.write({ set: [{ key: newRootId, value: JSON.stringify(newRoot) }] });
            this.rootId = newRootId;
            this.height = 1;
            
            // Update metadata with new root and height
            this.saveMetadata();
        } else {
            // Update parent node
            this.updateParentAfterSplit(nodeId, newNodeId, splitKey, path);
        }
    }

    private splitInternalNode(nodeId: string, node: InternalNode, path: NodePath[]): void {
        const mid = Math.floor(node.keys.length / 2);
        const splitKey = node.keys[mid];
        const newNodeId = INTERNAL_PREFIX + Date.now() + Math.random().toString(36).substring(2, 10);
        
        const newNode: InternalNode = {
            keys: node.keys.slice(mid + 1),
            children: node.children.slice(mid + 1)
        };
        
        node.keys = node.keys.slice(0, mid);
        node.children = node.children.slice(0, mid + 1);
        
        this.store.write({
            set: [
                { key: nodeId, value: JSON.stringify(node) },
                { key: newNodeId, value: JSON.stringify(newNode) }
            ]
        });
        
        if (path.length === 0) {
            // This is the root node, create a new root
            const newRoot: InternalNode = {
                keys: [splitKey],
                children: [nodeId, newNodeId]
            };
            const newRootId = INTERNAL_PREFIX + Date.now() + Math.random().toString(36).substring(2, 10);
            this.store.write({ set: [{ key: newRootId, value: JSON.stringify(newRoot) }] });
            this.rootId = newRootId;
            this.height++;
            
            // Update metadata with new root and height
            this.saveMetadata();
        } else {
            // Update parent
            const parentPath = path.slice(0, -1);
            const parentId = parentPath[parentPath.length - 1]?.id || this.rootId;
            const parent = JSON.parse(this.store.get(parentId) || "{}") as InternalNode;
            const childIndex = path[path.length - 1].index;
            
            // Insert the new node in the parent
            parent.keys.splice(childIndex, 0, splitKey);
            parent.children.splice(childIndex + 1, 0, newNodeId);
            
            if (parent.keys.length > this.maxInternalSize) {
                this.splitInternalNode(parentId, parent, parentPath);
            } else {
                this.store.write({ set: [{ key: parentId, value: JSON.stringify(parent) }] });
            }
        }
    }

    private updateParentAfterSplit(leftId: string, rightId: string, splitKey: string, path: NodePath[]): void {
        const parentPath = path.slice(0, -1);
        const parentId = parentPath[parentPath.length - 1]?.id || this.rootId;
        const parent = JSON.parse(this.store.get(parentId) || "{}") as InternalNode;
        const childIndex = path[path.length - 1].index;
        
        // Insert the new node in the parent
        parent.keys.splice(childIndex, 0, splitKey);
        parent.children.splice(childIndex + 1, 0, rightId);
        
        if (parent.keys.length > this.maxInternalSize) {
            this.splitInternalNode(parentId, parent, parentPath);
        } else {
            this.store.write({ set: [{ key: parentId, value: JSON.stringify(parent) }] });
        }
    }

    private rebalanceAfterDelete(nodeId: string, node: LeafNode | InternalNode, path: NodePath[]): void {
        const parentPath = path.slice(0, -1);
        const parentId = parentPath[parentPath.length - 1]?.id || this.rootId;
        const parent = JSON.parse(this.store.get(parentId) || "{}") as InternalNode;
        const childIndex = path[path.length - 1].index;
        
        // Try to borrow from left sibling
        if (childIndex > 0) {
            const leftSiblingId = parent.children[childIndex - 1];
            const leftSibling = JSON.parse(this.store.get(leftSiblingId) || "{}") as any;
            
            if (leftSibling.keys.length > Math.ceil(this.maxLeafSize / 2)) {
                this.borrowFromLeftSibling(nodeId, node, leftSiblingId, leftSibling, parentId, parent, childIndex);
                return;
            }
        }
        
        // Try to borrow from right sibling
        if (childIndex < parent.children.length - 1) {
            const rightSiblingId = parent.children[childIndex + 1];
            const rightSibling = JSON.parse(this.store.get(rightSiblingId) || "{}") as any;
            
            if (rightSibling.keys.length > Math.ceil(this.maxLeafSize / 2)) {
                this.borrowFromRightSibling(nodeId, node, rightSiblingId, rightSibling, parentId, parent, childIndex);
                return;
            }
        }
        
        // Merge with a sibling
        if (childIndex > 0) {
            // Merge with left sibling
            const leftSiblingId = parent.children[childIndex - 1];
            const leftSibling = JSON.parse(this.store.get(leftSiblingId) || "{}") as any;
            this.mergeWithLeftSibling(nodeId, node, leftSiblingId, leftSibling, parentId, parent, childIndex, parentPath);
        } else {
            // Merge with right sibling
            const rightSiblingId = parent.children[childIndex + 1];
            const rightSibling = JSON.parse(this.store.get(rightSiblingId) || "{}") as any;
            this.mergeWithRightSibling(nodeId, node, rightSiblingId, rightSibling, parentId, parent, childIndex, parentPath);
        }
    }

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
            const key = leftSibling.keys.pop();
            const value = leftSibling.values.pop();
            node.keys.unshift(key);
            node.values.unshift(value);
            
            // Update parent key
            parent.keys[childIndex - 1] = node.keys[0];
        } else {
            // For internal nodes
            // Move the rightmost child from left sibling to node
            const parentKey = parent.keys[childIndex - 1];
            const childId = leftSibling.children.pop();
            const key = leftSibling.keys.pop();
            
            node.keys.unshift(parentKey);
            node.children.unshift(childId);
            parent.keys[childIndex - 1] = key;
        }
        
        this.store.write({
            set: [
                { key: nodeId, value: JSON.stringify(node) },
                { key: leftSiblingId, value: JSON.stringify(leftSibling) },
                { key: parentId, value: JSON.stringify(parent) }
            ]
        });
    }

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
            const key = rightSibling.keys.shift();
            const value = rightSibling.values.shift();
            node.keys.push(key);
            node.values.push(value);
            
            // Update parent key
            parent.keys[childIndex] = rightSibling.keys[0];
        } else {
            // For internal nodes
            // Move the leftmost child from right sibling to node
            const parentKey = parent.keys[childIndex];
            const childId = rightSibling.children.shift();
            const key = rightSibling.keys.shift();
            
            node.keys.push(parentKey);
            node.children.push(childId);
            parent.keys[childIndex] = key;
        }
        
        this.store.write({
            set: [
                { key: nodeId, value: JSON.stringify(node) },
                { key: rightSiblingId, value: JSON.stringify(rightSibling) },
                { key: parentId, value: JSON.stringify(parent) }
            ]
        });
    }

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
            leftSibling.keys = [...leftSibling.keys, ...node.keys];
            leftSibling.values = [...leftSibling.values, ...node.values];
            leftSibling.next = node.next;
        } else {
            // For internal nodes
            // Include the parent key in the merge
            const parentKey = parent.keys[childIndex - 1];
            leftSibling.keys = [...leftSibling.keys, parentKey, ...node.keys];
            leftSibling.children = [...leftSibling.children, ...node.children];
        }
        
        // Remove the node and update parent
        parent.keys.splice(childIndex - 1, 1);
        parent.children.splice(childIndex, 1);
        
        this.store.write({
            set: [
                { key: leftSiblingId, value: JSON.stringify(leftSibling) },
                { key: parentId, value: JSON.stringify(parent) }
            ],
            delete: [nodeId]
        });
        
        // Check if parent needs rebalancing
        if (parent.keys.length < Math.ceil(this.maxInternalSize / 2) && parentPath.length > 0) {
            this.rebalanceAfterDelete(parentId, parent, parentPath);
        } else if (parent.keys.length === 0 && parentId === this.rootId) {
            // Root is empty, make the merged node the new root
            this.rootId = leftSiblingId;
            this.height--;
            this.store.write({ delete: [parentId] });
            
            // Update metadata with new root and height
            this.saveMetadata();
        }
    }

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
            node.keys = [...node.keys, ...rightSibling.keys];
            node.values = [...node.values, ...rightSibling.values];
            node.next = rightSibling.next;
        } else {
            // For internal nodes
            // Include the parent key in the merge
            const parentKey = parent.keys[childIndex];
            node.keys = [...node.keys, parentKey, ...rightSibling.keys];
            node.children = [...node.children, ...rightSibling.children];
        }
        
        // Remove the right sibling and update parent
        parent.keys.splice(childIndex, 1);
        parent.children.splice(childIndex + 1, 1);
        
        this.store.write({
            set: [
                { key: nodeId, value: JSON.stringify(node) },
                { key: parentId, value: JSON.stringify(parent) }
            ],
            delete: [rightSiblingId]
        });
        
        // Check if parent needs rebalancing
        if (parent.keys.length < Math.ceil(this.maxInternalSize / 2) && parentPath.length > 0) {
            this.rebalanceAfterDelete(parentId, parent, parentPath);
        } else if (parent.keys.length === 0 && parentId === this.rootId) {
            // Root is empty, make the merged node the new root
            this.rootId = nodeId;
            this.height--;
            this.store.write({ delete: [parentId] });
            
            // Update metadata with new root and height
            this.saveMetadata();
        }
    }

    private findNodePath(key: string): NodePath[] {
        const path: NodePath[] = [];
        let current = this.rootId;
        let level = 0;
        
        while (level < this.height) {
            const node = JSON.parse(this.store.get(current) || "{}") as InternalNode;
            const index = this.findChildIndex(node.keys, key);
            path.push({ id: current, index });
            current = node.children[index] || node.children[node.children.length - 1];
            level++;
        }
        
        path.push({ id: current, index: -1 }); // Leaf node doesn't need an index
        return path;
    }

    private findLeaf(key: string): string {
        let current = this.rootId;
        let level = 0;
        
        while (level < this.height) {
            const node = JSON.parse(this.store.get(current) || "{}") as InternalNode;
            const index = this.findChildIndex(node.keys, key);
            current = node.children[index] || node.children[node.children.length - 1];
            level++;
        }
        
        return current;
    }

    private findChildIndex(keys: string[], key: string): number {
        let left = 0;
        let right = keys.length;
        
        while (left < right) {
            const mid = Math.floor((left + right) / 2);
            if (keys[mid] <= key) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        
        return left;
    }

    private findInsertIndex(keys: string[], key: string): number {
        let left = 0;
        let right = keys.length;
        
        while (left < right) {
            const mid = Math.floor((left + right) / 2);
            if (keys[mid] < key) {
                left = mid + 1;
            } else if (keys[mid] === key) {
                return mid;
            } else {
                right = mid;
            }
        }
        
        return left;
    }
}
