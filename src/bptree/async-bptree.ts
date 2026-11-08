import { AsyncOrderedKeyValueApi, WriteArgs, ListArgs } from "../lib/types"
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
 * An asynchronous B+ Tree implementation on top of an ordered key-value store.
 * This implementation uses a prefix-based approach for storing nodes:
 * - Leaf nodes are prefixed with "00"
 * - Internal nodes are prefixed with "01"
 */
export class AsyncBPlusTree implements AsyncOrderedKeyValueApi<string, string> {
	private rootId: string = ""
	private height: number = 0
	private initialized: boolean = false

	constructor(
		private store: AsyncOrderedKeyValueApi<string, string>,
		private maxLeafSize: number = DEFAULT_MAX_LEAF_SIZE,
		private maxInternalSize: number = DEFAULT_MAX_INTERNAL_SIZE
	) {}

	async init(): Promise<void> {
		const metadataStr = await this.store.get(METADATA_KEY)

		if (metadataStr) {
			const metadata = JSON.parse(metadataStr) as TreeMetadata
			this.rootId = metadata.rootId
			this.height = metadata.height
		} else {
			const rootNode: LeafNode = { keys: [], values: [] }
			this.rootId = ROOT_LEAF_ID
			this.height = 0

			await this.store.write({
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

	private async saveMetadata(): Promise<void> {
		const metadata: TreeMetadata = {
			rootId: this.rootId,
			height: this.height,
		}

		await this.store.write({
			set: [{ key: METADATA_KEY, value: JSON.stringify(metadata) }],
		})
	}

	private async readNode<T extends TreeNode>(nodeId: string): Promise<T> {
		const nodeStr = await this.store.get(nodeId)
		return JSON.parse(nodeStr || "{}") as T
	}

	private async writeNode(nodeId: string, node: TreeNode): Promise<void> {
		await this.store.write({
			set: [{ key: nodeId, value: JSON.stringify(node) }],
		})
	}

	async get(key: string): Promise<string | undefined> {
		if (!this.initialized) await this.init()

		const leafNode = await this.findLeaf(key)
		const nodeStr = await this.store.get(leafNode)
		if (!nodeStr) return undefined

		const node = JSON.parse(nodeStr) as LeafNode
		const index = node.keys.indexOf(key)
		return index !== -1 ? node.values[index] : undefined
	}

	async list(
		args: ListArgs<string> = {}
	): Promise<{ key: string; value: string }[]> {
		if (!this.initialized) await this.init()

		const results: { key: string; value: string }[] = []
		let current = await this.findLeaf(args.gte || args.gt || "")

		while (current && current !== "") {
			const nodeStr = await this.store.get(current)
			if (!nodeStr) break

			const node = JSON.parse(nodeStr) as LeafNode
			if (!collectLeafRangePage(node, args, results)) {
				break
			}
			current = node.next || ""
		}

		return finalizeRangeResults(results, args)
	}

	async set(key: string, value: string): Promise<void> {
		if (!this.initialized) await this.init()
		await this.write({ set: [{ key, value }] })
	}

	async delete(key: string): Promise<void> {
		if (!this.initialized) await this.init()
		await this.write({ delete: [key] })
	}

	async write(tx: WriteArgs<string, string>): Promise<void> {
		if (!this.initialized) await this.init()

		for (const { key, value } of tx.set || []) {
			await this.insert(key, value)
		}
		for (const key of tx.delete || []) {
			await this.remove(key)
		}
	}

	private async insert(key: string, value: string): Promise<void> {
		const path = await this.findNodePath(key)
		const leafId = path[path.length - 1].id
		const leafNode = await this.readNode<LeafNode>(leafId)

		if (!upsertLeafEntry(leafNode, key, value)) {
			await this.writeNode(leafId, leafNode)
			return
		}

		if (leafNode.keys.length > this.maxLeafSize) {
			await this.splitLeafNode(leafId, leafNode, path)
		} else {
			await this.writeNode(leafId, leafNode)
		}
	}

	private async remove(key: string): Promise<void> {
		const path = await this.findNodePath(key)
		const leafId = path[path.length - 1].id
		const leafNode = await this.readNode<LeafNode>(leafId)

		if (!removeLeafEntry(leafNode, key)) return

		if (
			leafNode.keys.length < Math.ceil(this.maxLeafSize / 2) &&
			path.length > 1
		) {
			await this.rebalanceAfterDelete(leafId, leafNode, path)
		} else {
			await this.writeNode(leafId, leafNode)
		}
	}

	private async splitLeafNode(
		nodeId: string,
		node: LeafNode,
		path: NodePath[] = []
	): Promise<void> {
		const newNodeId = createNodeId(LEAF_PREFIX)
		const { splitKey, newNode } = partitionLeafNode(node, newNodeId)

		await this.store.write({
			set: [
				{ key: nodeId, value: JSON.stringify(node) },
				{ key: newNodeId, value: JSON.stringify(newNode) },
			],
		})

		if (path.length <= 1) {
			await this.promoteNewRoot(nodeId, newNodeId, splitKey, 1)
		} else {
			await this.updateParentAfterSplit(newNodeId, splitKey, path)
		}
	}

	private async splitInternalNode(
		nodeId: string,
		node: InternalNode,
		path: NodePath[]
	): Promise<void> {
		const newNodeId = createNodeId(INTERNAL_PREFIX)
		const { splitKey, newNode } = partitionInternalNode(node)

		await this.store.write({
			set: [
				{ key: nodeId, value: JSON.stringify(node) },
				{ key: newNodeId, value: JSON.stringify(newNode) },
			],
		})

		if (path.length <= 1) {
			await this.promoteNewRoot(nodeId, newNodeId, splitKey, this.height + 1)
		} else {
			await this.updateParentAfterSplit(newNodeId, splitKey, path)
		}
	}

	private async promoteNewRoot(
		leftId: string,
		rightId: string,
		splitKey: string,
		nextHeight: number
	): Promise<void> {
		const newRoot = createRootInternalNode(splitKey, leftId, rightId)
		const newRootId = createNodeId(INTERNAL_PREFIX)
		await this.writeNode(newRootId, newRoot)
		this.rootId = newRootId
		this.height = nextHeight
		await this.saveMetadata()
	}

	private async updateParentAfterSplit(
		rightId: string,
		splitKey: string,
		path: NodePath[]
	): Promise<void> {
		const parentPath = path.slice(0, -1)
		const parentEntry = parentPath[parentPath.length - 1]
		const parentId = parentEntry?.id || this.rootId
		const parent = await this.readNode<InternalNode>(parentId)

		insertIntoInternalNode(parent, parentEntry.index, splitKey, rightId)

		if (parent.keys.length > this.maxInternalSize) {
			await this.splitInternalNode(parentId, parent, parentPath)
		} else {
			await this.writeNode(parentId, parent)
		}
	}

	private async rebalanceAfterDelete(
		nodeId: string,
		node: TreeNode,
		path: NodePath[]
	): Promise<void> {
		const parentPath = path.slice(0, -1)
		const parentEntry = parentPath[parentPath.length - 1]
		const parentId = parentEntry?.id || this.rootId
		const parent = await this.readNode<InternalNode>(parentId)
		const childIndex = parentEntry.index
		const minKeys = minKeysForNode(node, this.maxLeafSize, this.maxInternalSize)

		if (childIndex > 0) {
			const leftSiblingId = parent.children[childIndex - 1]
			const leftSibling = await this.readNode<TreeNode>(leftSiblingId)

			if (leftSibling.keys.length > minKeys) {
				borrowFromLeftSiblingNode(node, leftSibling, parent, childIndex)
				await this.store.write({
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
			const rightSibling = await this.readNode<TreeNode>(rightSiblingId)

			if (rightSibling.keys.length > minKeys) {
				borrowFromRightSiblingNode(node, rightSibling, parent, childIndex)
				await this.store.write({
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
			const leftSibling = await this.readNode<TreeNode>(leftSiblingId)
			mergeWithLeftSiblingNode(node, leftSibling, parent, childIndex)
			await this.commitMerge(
				leftSiblingId,
				leftSibling,
				nodeId,
				parentId,
				parent,
				parentPath
			)
		} else {
			const rightSiblingId = parent.children[childIndex + 1]
			const rightSibling = await this.readNode<TreeNode>(rightSiblingId)
			mergeWithRightSiblingNode(node, rightSibling, parent, childIndex)
			await this.commitMerge(
				nodeId,
				node,
				rightSiblingId,
				parentId,
				parent,
				parentPath
			)
		}
	}

	private async commitMerge(
		survivingId: string,
		survivingNode: TreeNode,
		removedId: string,
		parentId: string,
		parent: InternalNode,
		parentPath: NodePath[]
	): Promise<void> {
		await this.store.write({
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
			await this.rebalanceAfterDelete(parentId, parent, parentPath)
		} else if (parent.keys.length === 0 && parentId === this.rootId) {
			this.rootId = survivingId
			this.height--
			await this.store.write({ delete: [parentId] })
			await this.saveMetadata()
		}
	}

	private async findNodePath(key: string): Promise<NodePath[]> {
		const path: NodePath[] = []
		let current = this.rootId
		let level = 0

		while (level < this.height) {
			const node = await this.readNode<InternalNode>(current)
			const index = findChildIndex(node.keys, key)
			path.push({ id: current, index })
			current = node.children[index] || node.children[node.children.length - 1]
			level++
		}

		path.push({ id: current, index: -1 })
		return path
	}

	private async findLeaf(key: string): Promise<string> {
		let current = this.rootId
		let level = 0

		while (level < this.height) {
			const node = await this.readNode<InternalNode>(current)
			const index = findChildIndex(node.keys, key)
			current = node.children[index] || node.children[node.children.length - 1]
			level++
		}

		return current
	}
}
