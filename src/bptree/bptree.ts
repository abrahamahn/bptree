import { OrderedKeyValueApi, WriteArgs, ListArgs } from "../lib/types"
import {
	DEFAULT_MAX_INTERNAL_SIZE,
	DEFAULT_MAX_LEAF_SIZE,
	INTERNAL_PREFIX,
	InternalNode,
	LEAF_PREFIX,
	LeafNode,
	METADATA_KEY,
	NodePath,
	ROOT_LEAF_ID,
	TreeMetadata,
	TreeNode,
	borrowFromLeftSiblingNode,
	borrowFromRightSiblingNode,
	collectLeafRangePage,
	createNodeId,
	createRootInternalNode,
	finalizeRangeResults,
	findChildIndex,
	insertIntoInternalNode,
	mergeWithLeftSiblingNode,
	mergeWithRightSiblingNode,
	minKeysForNode,
	partitionInternalNode,
	partitionLeafNode,
	removeLeafEntry,
	upsertLeafEntry,
} from "./node"

/**
 * A B+ Tree implementation on top of an ordered key-value store.
 * This implementation uses a prefix-based approach for storing nodes:
 * - Leaf nodes are prefixed with "00"
 * - Internal nodes are prefixed with "01"
 */
export class BPlusTree implements OrderedKeyValueApi<string, string> {
	private rootId: string = ""
	private height: number = 0
	private initialized: boolean = false

	constructor(
		private store: OrderedKeyValueApi<string, string>,
		private maxLeafSize: number = DEFAULT_MAX_LEAF_SIZE,
		private maxInternalSize: number = DEFAULT_MAX_INTERNAL_SIZE
	) {
		this.init()
	}

	private init(): void {
		const metadataStr = this.store.get(METADATA_KEY)

		if (metadataStr) {
			const metadata = JSON.parse(metadataStr) as TreeMetadata
			this.rootId = metadata.rootId
			this.height = metadata.height
		} else {
			const rootNode: LeafNode = { keys: [], values: [] }
			this.rootId = ROOT_LEAF_ID
			this.height = 0

			this.store.write({
				set: [
					{ key: this.rootId, value: JSON.stringify(rootNode) },
					{
						key: METADATA_KEY,
						value: JSON.stringify({ rootId: this.rootId, height: this.height }),
					},
				],
			})
		}

		this.initialized = true
	}

	private saveMetadata(): void {
		const metadata: TreeMetadata = {
			rootId: this.rootId,
			height: this.height,
		}

		this.store.write({
			set: [{ key: METADATA_KEY, value: JSON.stringify(metadata) }],
		})
	}

	private readNode<T extends TreeNode>(nodeId: string): T {
		return JSON.parse(this.store.get(nodeId) || "{}") as T
	}

	private writeNode(nodeId: string, node: TreeNode): void {
		this.store.write({
			set: [{ key: nodeId, value: JSON.stringify(node) }],
		})
	}

	get(key: string): string | undefined {
		if (!this.initialized) this.init()

		const leafNode = this.findLeaf(key)
		const node = this.readNode<LeafNode>(leafNode)
		const index = node.keys.indexOf(key)
		return index !== -1 ? node.values[index] : undefined
	}

	list(args: ListArgs<string> = {}): { key: string; value: string }[] {
		if (!this.initialized) this.init()

		const results: { key: string; value: string }[] = []
		let current = this.findLeaf(args.gte || args.gt || "")

		while (current && current !== "") {
			const node = this.readNode<LeafNode>(current)
			if (!collectLeafRangePage(node, args, results)) {
				break
			}
			current = node.next || ""
		}

		return finalizeRangeResults(results, args)
	}

	set(key: string, value: string): void {
		if (!this.initialized) this.init()
		this.write({ set: [{ key, value }] })
	}

	delete(key: string): void {
		if (!this.initialized) this.init()
		this.write({ delete: [key] })
	}

	write(tx: WriteArgs<string, string>): void {
		if (!this.initialized) this.init()

		for (const { key, value } of tx.set || []) {
			this.insert(key, value)
		}
		for (const key of tx.delete || []) {
			this.remove(key)
		}
	}

	private insert(key: string, value: string): void {
		const path = this.findNodePath(key)
		const leafId = path[path.length - 1].id
		const leafNode = this.readNode<LeafNode>(leafId)

		if (!upsertLeafEntry(leafNode, key, value)) {
			this.writeNode(leafId, leafNode)
			return
		}

		if (leafNode.keys.length > this.maxLeafSize) {
			this.splitLeafNode(leafId, leafNode, path)
		} else {
			this.writeNode(leafId, leafNode)
		}
	}

	private remove(key: string): void {
		const path = this.findNodePath(key)
		const leafId = path[path.length - 1].id
		const leafNode = this.readNode<LeafNode>(leafId)

		if (!removeLeafEntry(leafNode, key)) return

		if (
			leafNode.keys.length < Math.ceil(this.maxLeafSize / 2) &&
			path.length > 1
		) {
			this.rebalanceAfterDelete(leafId, leafNode, path)
		} else {
			this.writeNode(leafId, leafNode)
		}
	}

	private splitLeafNode(
		nodeId: string,
		node: LeafNode,
		path: NodePath[] = []
	): void {
		const newNodeId = createNodeId(LEAF_PREFIX)
		const { splitKey, newNode } = partitionLeafNode(node, newNodeId)

		this.store.write({
			set: [
				{ key: nodeId, value: JSON.stringify(node) },
				{ key: newNodeId, value: JSON.stringify(newNode) },
			],
		})

		if (path.length <= 1) {
			this.promoteNewRoot(nodeId, newNodeId, splitKey, 1)
		} else {
			this.updateParentAfterSplit(newNodeId, splitKey, path)
		}
	}

	private splitInternalNode(
		nodeId: string,
		node: InternalNode,
		path: NodePath[]
	): void {
		const newNodeId = createNodeId(INTERNAL_PREFIX)
		const { splitKey, newNode } = partitionInternalNode(node)

		this.store.write({
			set: [
				{ key: nodeId, value: JSON.stringify(node) },
				{ key: newNodeId, value: JSON.stringify(newNode) },
			],
		})

		if (path.length <= 1) {
			this.promoteNewRoot(nodeId, newNodeId, splitKey, this.height + 1)
		} else {
			this.updateParentAfterSplit(newNodeId, splitKey, path)
		}
	}

	private promoteNewRoot(
		leftId: string,
		rightId: string,
		splitKey: string,
		nextHeight: number
	): void {
		const newRoot = createRootInternalNode(splitKey, leftId, rightId)
		const newRootId = createNodeId(INTERNAL_PREFIX)
		this.writeNode(newRootId, newRoot)
		this.rootId = newRootId
		this.height = nextHeight
		this.saveMetadata()
	}

	private updateParentAfterSplit(
		rightId: string,
		splitKey: string,
		path: NodePath[]
	): void {
		const parentPath = path.slice(0, -1)
		const parentEntry = parentPath[parentPath.length - 1]
		const parentId = parentEntry?.id || this.rootId
		const parent = this.readNode<InternalNode>(parentId)

		insertIntoInternalNode(parent, parentEntry.index, splitKey, rightId)

		if (parent.keys.length > this.maxInternalSize) {
			this.splitInternalNode(parentId, parent, parentPath)
		} else {
			this.writeNode(parentId, parent)
		}
	}

	private rebalanceAfterDelete(
		nodeId: string,
		node: TreeNode,
		path: NodePath[]
	): void {
		const parentPath = path.slice(0, -1)
		const parentEntry = parentPath[parentPath.length - 1]
		const parentId = parentEntry?.id || this.rootId
		const parent = this.readNode<InternalNode>(parentId)
		const childIndex = parentEntry.index
		const minKeys = minKeysForNode(node, this.maxLeafSize, this.maxInternalSize)

		if (childIndex > 0) {
			const leftSiblingId = parent.children[childIndex - 1]
			const leftSibling = this.readNode<TreeNode>(leftSiblingId)

			if (leftSibling.keys.length > minKeys) {
				borrowFromLeftSiblingNode(node, leftSibling, parent, childIndex)
				this.store.write({
					set: [
						{ key: nodeId, value: JSON.stringify(node) },
						{ key: leftSiblingId, value: JSON.stringify(leftSibling) },
						{ key: parentId, value: JSON.stringify(parent) },
					],
				})
				return
			}
		}

		if (childIndex < parent.children.length - 1) {
			const rightSiblingId = parent.children[childIndex + 1]
			const rightSibling = this.readNode<TreeNode>(rightSiblingId)

			if (rightSibling.keys.length > minKeys) {
				borrowFromRightSiblingNode(node, rightSibling, parent, childIndex)
				this.store.write({
					set: [
						{ key: nodeId, value: JSON.stringify(node) },
						{ key: rightSiblingId, value: JSON.stringify(rightSibling) },
						{ key: parentId, value: JSON.stringify(parent) },
					],
				})
				return
			}
		}

		if (childIndex > 0) {
			const leftSiblingId = parent.children[childIndex - 1]
			const leftSibling = this.readNode<TreeNode>(leftSiblingId)
			mergeWithLeftSiblingNode(node, leftSibling, parent, childIndex)
			this.commitMerge(
				leftSiblingId,
				leftSibling,
				nodeId,
				parentId,
				parent,
				parentPath
			)
		} else {
			const rightSiblingId = parent.children[childIndex + 1]
			const rightSibling = this.readNode<TreeNode>(rightSiblingId)
			mergeWithRightSiblingNode(node, rightSibling, parent, childIndex)
			this.commitMerge(nodeId, node, rightSiblingId, parentId, parent, parentPath)
		}
	}

	private commitMerge(
		survivingId: string,
		survivingNode: TreeNode,
		removedId: string,
		parentId: string,
		parent: InternalNode,
		parentPath: NodePath[]
	): void {
		this.store.write({
			set: [
				{ key: survivingId, value: JSON.stringify(survivingNode) },
				{ key: parentId, value: JSON.stringify(parent) },
			],
			delete: [removedId],
		})

		if (
			parent.keys.length < Math.ceil(this.maxInternalSize / 2) &&
			parentPath.length > 1
		) {
			this.rebalanceAfterDelete(parentId, parent, parentPath)
		} else if (parent.keys.length === 0 && parentId === this.rootId) {
			this.rootId = survivingId
			this.height--
			this.store.write({ delete: [parentId] })
			this.saveMetadata()
		}
	}

	private findNodePath(key: string): NodePath[] {
		const path: NodePath[] = []
		let current = this.rootId
		let level = 0

		while (level < this.height) {
			const node = this.readNode<InternalNode>(current)
			const index = findChildIndex(node.keys, key)
			path.push({ id: current, index })
			current = node.children[index] || node.children[node.children.length - 1]
			level++
		}

		path.push({ id: current, index: -1 })
		return path
	}

	private findLeaf(key: string): string {
		let current = this.rootId
		let level = 0

		while (level < this.height) {
			const node = this.readNode<InternalNode>(current)
			const index = findChildIndex(node.keys, key)
			current = node.children[index] || node.children[node.children.length - 1]
			level++
		}

		return current
	}
}
