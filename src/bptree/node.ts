import { ListArgs } from "../lib/types"

export const LEAF_PREFIX = "00"
export const INTERNAL_PREFIX = "01"
export const METADATA_KEY = "__BPTREE_METADATA__"
export const ROOT_LEAF_ID = LEAF_PREFIX + "root"
export const DEFAULT_MAX_LEAF_SIZE = 32
export const DEFAULT_MAX_INTERNAL_SIZE = 32

export type LeafNode = {
	keys: string[]
	values: string[]
	next?: string
}

export type InternalNode = {
	keys: string[]
	children: string[]
}

export type TreeNode = LeafNode | InternalNode

export type NodePath = {
	id: string
	index: number
}

export type TreeMetadata = {
	rootId: string
	height: number
}

export function createNodeId(prefix: string): string {
	return prefix + Date.now() + Math.random().toString(36).substring(2, 10)
}

export function isLeafNode(node: TreeNode): node is LeafNode {
	return "values" in node
}

export function minKeysForNode(
	node: TreeNode,
	maxLeafSize: number,
	maxInternalSize: number
): number {
	return isLeafNode(node)
		? Math.ceil(maxLeafSize / 2)
		: Math.ceil(maxInternalSize / 2)
}

export function findChildIndex(keys: string[], key: string): number {
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

export function findInsertIndex(keys: string[], key: string): number {
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

/**
 * Inserts or updates a key-value pair in a leaf node.
 * Returns true if a new key was inserted, or false if an existing key was updated in place.
 */
export function upsertLeafEntry(
	node: LeafNode,
	key: string,
	value: string
): boolean {
	const index = findInsertIndex(node.keys, key)
	if (index < node.keys.length && node.keys[index] === key) {
		node.values[index] = value
		return false
	}
	node.keys.splice(index, 0, key)
	node.values.splice(index, 0, value)
	return true
}

/**
 * Removes a key-value pair from a leaf node.
 * Returns true if the key was found and removed, or false otherwise.
 */
export function removeLeafEntry(node: LeafNode, key: string): boolean {
	const index = node.keys.indexOf(key)
	if (index === -1) return false
	node.keys.splice(index, 1)
	node.values.splice(index, 1)
	return true
}

export function partitionLeafNode(
	node: LeafNode,
	newNodeId: string
): { splitKey: string; newNode: LeafNode } {
	const mid = Math.floor(node.keys.length / 2)
	const newNode: LeafNode = {
		keys: node.keys.slice(mid),
		values: node.values.slice(mid),
		next: node.next,
	}
	node.keys = node.keys.slice(0, mid)
	node.values = node.values.slice(0, mid)
	node.next = newNodeId
	return { splitKey: newNode.keys[0], newNode }
}

export function partitionInternalNode(node: InternalNode): {
	splitKey: string
	newNode: InternalNode
} {
	const mid = Math.floor(node.keys.length / 2)
	const splitKey = node.keys[mid]
	const newNode: InternalNode = {
		keys: node.keys.slice(mid + 1),
		children: node.children.slice(mid + 1),
	}
	node.keys = node.keys.slice(0, mid)
	node.children = node.children.slice(0, mid + 1)
	return { splitKey, newNode }
}

export function createRootInternalNode(
	splitKey: string,
	leftId: string,
	rightId: string
): InternalNode {
	return {
		keys: [splitKey],
		children: [leftId, rightId],
	}
}

export function insertIntoInternalNode(
	parent: InternalNode,
	childIndex: number,
	splitKey: string,
	rightId: string
): void {
	parent.keys.splice(childIndex, 0, splitKey)
	parent.children.splice(childIndex + 1, 0, rightId)
}

export function borrowFromLeftSiblingNode(
	node: TreeNode,
	leftSibling: TreeNode,
	parent: InternalNode,
	childIndex: number
): void {
	if (isLeafNode(node) && isLeafNode(leftSibling)) {
		const key = leftSibling.keys.pop()!
		const value = leftSibling.values.pop()!
		node.keys.unshift(key)
		node.values.unshift(value)
		parent.keys[childIndex - 1] = node.keys[0]
	} else if (!isLeafNode(node) && !isLeafNode(leftSibling)) {
		const parentKey = parent.keys[childIndex - 1]
		const childId = leftSibling.children.pop()!
		const key = leftSibling.keys.pop()!
		node.keys.unshift(parentKey)
		node.children.unshift(childId)
		parent.keys[childIndex - 1] = key
	}
}

export function borrowFromRightSiblingNode(
	node: TreeNode,
	rightSibling: TreeNode,
	parent: InternalNode,
	childIndex: number
): void {
	if (isLeafNode(node) && isLeafNode(rightSibling)) {
		const key = rightSibling.keys.shift()!
		const value = rightSibling.values.shift()!
		node.keys.push(key)
		node.values.push(value)
		parent.keys[childIndex] = rightSibling.keys[0]
	} else if (!isLeafNode(node) && !isLeafNode(rightSibling)) {
		const parentKey = parent.keys[childIndex]
		const childId = rightSibling.children.shift()!
		const key = rightSibling.keys.shift()!
		node.keys.push(parentKey)
		node.children.push(childId)
		parent.keys[childIndex] = key
	}
}

export function mergeWithLeftSiblingNode(
	node: TreeNode,
	leftSibling: TreeNode,
	parent: InternalNode,
	childIndex: number
): void {
	if (isLeafNode(node) && isLeafNode(leftSibling)) {
		leftSibling.keys = [...leftSibling.keys, ...node.keys]
		leftSibling.values = [...leftSibling.values, ...node.values]
		leftSibling.next = node.next
	} else if (!isLeafNode(node) && !isLeafNode(leftSibling)) {
		const parentKey = parent.keys[childIndex - 1]
		leftSibling.keys = [...leftSibling.keys, parentKey, ...node.keys]
		leftSibling.children = [...leftSibling.children, ...node.children]
	}
	parent.keys.splice(childIndex - 1, 1)
	parent.children.splice(childIndex, 1)
}

export function mergeWithRightSiblingNode(
	node: TreeNode,
	rightSibling: TreeNode,
	parent: InternalNode,
	childIndex: number
): void {
	if (isLeafNode(node) && isLeafNode(rightSibling)) {
		node.keys = [...node.keys, ...rightSibling.keys]
		node.values = [...node.values, ...rightSibling.values]
		node.next = rightSibling.next
	} else if (!isLeafNode(node) && !isLeafNode(rightSibling)) {
		const parentKey = parent.keys[childIndex]
		node.keys = [...node.keys, parentKey, ...rightSibling.keys]
		node.children = [...node.children, ...rightSibling.children]
	}
	parent.keys.splice(childIndex, 1)
	parent.children.splice(childIndex + 1, 1)
}

/**
 * Appends matching entries from `node` into `results`.
 * Returns `true` if scanning should continue to `node.next`, or `false` if the scan should stop.
 */
export function collectLeafRangePage(
	node: LeafNode,
	args: ListArgs<string>,
	results: { key: string; value: string }[]
): boolean {
	const startIndex = args.gt
		? node.keys.findIndex((k) => k > args.gt!)
		: args.gte
			? node.keys.findIndex((k) => k >= args.gte!)
			: 0

	if (startIndex === -1) {
		return true
	}

	for (let i = startIndex; i < node.keys.length; i++) {
		const key = node.keys[i]
		if (args.lt && key >= args.lt) return false
		if (args.lte && key > args.lte) return false

		results.push({ key, value: node.values[i] })
		if (
			!args.reverse &&
			!args.offset &&
			args.limit &&
			results.length >= args.limit
		) {
			return false
		}
	}

	return true
}

export function finalizeRangeResults(
	results: { key: string; value: string }[],
	args: ListArgs<string>
): { key: string; value: string }[] {
	if (args.reverse) {
		results.reverse()
	}
	if (args.offset && args.offset > 0) {
		results.splice(0, args.offset)
	}
	if (args.limit !== undefined) {
		results.splice(args.limit)
	}
	return results
}
