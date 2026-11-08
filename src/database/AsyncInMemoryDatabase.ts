import { AsyncOrderedKeyValueApi, ListArgs, WriteArgs } from "../lib/types"

export class AsyncInMemoryDatabase<V = any>
	implements AsyncOrderedKeyValueApi<string, V>
{
	private data: Map<string, V> = new Map()

	async get(key: string): Promise<V | undefined> {
		return this.data.get(key)
	}

	async list(args: ListArgs<string> = {}): Promise<{ key: string; value: V }[]> {
		let items = Array.from(this.data.entries()).map(([key, value]) => ({
			key,
			value,
		}))

		items.sort((a, b) => a.key.localeCompare(b.key))

		if (args.gt !== undefined) {
			items = items.filter((item) => item.key > args.gt!)
		}
		if (args.gte !== undefined) {
			items = items.filter((item) => item.key >= args.gte!)
		}
		if (args.lt !== undefined) {
			items = items.filter((item) => item.key < args.lt!)
		}
		if (args.lte !== undefined) {
			items = items.filter((item) => item.key <= args.lte!)
		}

		if (args.reverse) {
			items.reverse()
		}

		if (args.offset) {
			items = items.slice(args.offset)
		}

		if (args.limit) {
			items = items.slice(0, args.limit)
		}

		return items
	}

	async set(key: string, value: V): Promise<void> {
		await this.write({ set: [{ key, value }] })
	}

	async delete(key: string): Promise<void> {
		await this.write({ delete: [key] })
	}

	async write(tx: WriteArgs<string, V>): Promise<void> {
		if (tx.set) {
			for (const { key, value } of tx.set) {
				this.data.set(key, value)
			}
		}

		if (tx.delete) {
			for (const key of tx.delete) {
				this.data.delete(key)
			}
		}
	}

	async close(): Promise<void> {
		// No-op for in-memory database
	}
}
