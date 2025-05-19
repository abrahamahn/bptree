import { describe, it, beforeEach } from 'mocha';
import { expect } from "chai";
import { AsyncBPlusTree } from "./async-bptree";
import { AsyncOrderedKeyValueApi } from "../lib/types";

// Mock implementation of AsyncOrderedKeyValueApi for testing
class MockAsyncStorage implements AsyncOrderedKeyValueApi<string, string> {
    private data: Map<string, string> = new Map();

    async get(key: string): Promise<string | undefined> {
        return this.data.get(key);
    }

    async list(args: {
        gt?: string;
        gte?: string;
        lt?: string;
        lte?: string;
        limit?: number;
        offset?: number;
        reverse?: boolean;
    } = {}): Promise<{ key: string; value: string }[]> {
        // Convert map to array
        let items = Array.from(this.data.entries()).map(([key, value]) => ({ key, value }));
        
        // Sort by key
        items.sort((a, b) => a.key.localeCompare(b.key));
        
        // Apply range filters
        if (args.gt !== undefined) {
            items = items.filter(item => item.key > args.gt!);
        }
        if (args.gte !== undefined) {
            items = items.filter(item => item.key >= args.gte!);
        }
        if (args.lt !== undefined) {
            items = items.filter(item => item.key < args.lt!);
        }
        if (args.lte !== undefined) {
            items = items.filter(item => item.key <= args.lte!);
        }
        
        // Apply reverse if needed
        if (args.reverse) {
            items.reverse();
        }
        
        // Apply offset if needed
        if (args.offset) {
            items = items.slice(args.offset);
        }
        
        // Apply limit if needed
        if (args.limit) {
            items = items.slice(0, args.limit);
        }
        
        return items;
    }

    async set(key: string, value: string): Promise<void> {
        await this.write({ set: [{ key, value }] });
    }

    async delete(key: string): Promise<void> {
        await this.write({ delete: [key] });
    }

    async write(tx: { set?: { key: string; value: string }[]; delete?: string[] }): Promise<void> {
        if (tx.set) {
            for (const { key, value } of tx.set) {
                this.data.set(key, value);
            }
        }
        
        if (tx.delete) {
            for (const key of tx.delete) {
                this.data.delete(key);
            }
        }
    }
}

describe("AsyncBPlusTree with MockAsyncStorage", () => {
    let bPlusTree: AsyncBPlusTree;
    let store: MockAsyncStorage;
    
    beforeEach(async () => {
        // Use our mock async storage as the underlying storage
        store = new MockAsyncStorage();
        bPlusTree = new AsyncBPlusTree(store);
        await bPlusTree.init(); // Initialize the tree
    });
    
    describe("Basic CRUD operations", () => {
        it("should set and get values correctly", async () => {
            await bPlusTree.set("key1", "value1");
            await bPlusTree.set("key2", "value2");
            await bPlusTree.set("key3", "value3");
            
            expect(await bPlusTree.get("key1")).to.equal("value1");
            expect(await bPlusTree.get("key2")).to.equal("value2");
            expect(await bPlusTree.get("key3")).to.equal("value3");
            expect(await bPlusTree.get("keyNotFound")).to.be.undefined;
        });
        
        it("should update values correctly", async () => {
            await bPlusTree.set("key1", "value1");
            expect(await bPlusTree.get("key1")).to.equal("value1");
            
            await bPlusTree.set("key1", "updatedValue");
            expect(await bPlusTree.get("key1")).to.equal("updatedValue");
        });
        
        it("should delete values correctly", async () => {
            await bPlusTree.set("key1", "value1");
            await bPlusTree.set("key2", "value2");
            
            expect(await bPlusTree.get("key1")).to.equal("value1");
            expect(await bPlusTree.get("key2")).to.equal("value2");
            
            await bPlusTree.delete("key1");
            
            expect(await bPlusTree.get("key1")).to.be.undefined;
            expect(await bPlusTree.get("key2")).to.equal("value2");
        });
        
        it("should handle batch operations with write", async () => {
            await bPlusTree.write({
                set: [
                    { key: "key1", value: "value1" },
                    { key: "key2", value: "value2" },
                    { key: "key3", value: "value3" }
                ]
            });
            
            expect(await bPlusTree.get("key1")).to.equal("value1");
            expect(await bPlusTree.get("key2")).to.equal("value2");
            expect(await bPlusTree.get("key3")).to.equal("value3");
            
            await bPlusTree.write({
                set: [{ key: "key4", value: "value4" }],
                delete: ["key1", "key3"]
            });
            
            expect(await bPlusTree.get("key1")).to.be.undefined;
            expect(await bPlusTree.get("key2")).to.equal("value2");
            expect(await bPlusTree.get("key3")).to.be.undefined;
            expect(await bPlusTree.get("key4")).to.equal("value4");
        });
    });
    
    describe("Range queries", () => {
        beforeEach(async () => {
            // Insert values in non-sorted order
            const data = [
                { key: "e", value: "valueE" },
                { key: "c", value: "valueC" },
                { key: "a", value: "valueA" },
                { key: "d", value: "valueD" },
                { key: "b", value: "valueB" },
                { key: "f", value: "valueF" },
            ];
            
            await bPlusTree.write({ set: data });
        });
        
        it("should return all elements when no range is specified", async () => {
            const result = await bPlusTree.list();
            expect(result).to.have.lengthOf(6);
            expect(result[0].key).to.equal("a");
            expect(result[1].key).to.equal("b");
            expect(result[2].key).to.equal("c");
            expect(result[3].key).to.equal("d");
            expect(result[4].key).to.equal("e");
            expect(result[5].key).to.equal("f");
        });
        
        it("should handle gt/gte bounds correctly", async () => {
            let result = await bPlusTree.list({ gt: "c" });
            expect(result).to.have.lengthOf(3);
            expect(result[0].key).to.equal("d");
            expect(result[2].key).to.equal("f");
            
            result = await bPlusTree.list({ gte: "c" });
            expect(result).to.have.lengthOf(4);
            expect(result[0].key).to.equal("c");
            expect(result[3].key).to.equal("f");
        });
        
        it("should handle lt/lte bounds correctly", async () => {
            let result = await bPlusTree.list({ lt: "d" });
            expect(result).to.have.lengthOf(3);
            expect(result[0].key).to.equal("a");
            expect(result[2].key).to.equal("c");
            
            result = await bPlusTree.list({ lte: "d" });
            expect(result).to.have.lengthOf(4);
            expect(result[0].key).to.equal("a");
            expect(result[3].key).to.equal("d");
        });
        
        it("should handle combined range bounds correctly", async () => {
            let result = await bPlusTree.list({ gt: "b", lt: "e" });
            expect(result).to.have.lengthOf(2);
            expect(result[0].key).to.equal("c");
            expect(result[1].key).to.equal("d");
            
            result = await bPlusTree.list({ gte: "b", lte: "e" });
            expect(result).to.have.lengthOf(4);
            expect(result[0].key).to.equal("b");
            expect(result[3].key).to.equal("e");
        });
        
        it("should limit results correctly", async () => {
            const result = await bPlusTree.list({ limit: 3 });
            expect(result).to.have.lengthOf(3);
            expect(result[0].key).to.equal("a");
            expect(result[1].key).to.equal("b");
            expect(result[2].key).to.equal("c");
        });
        
        it("should handle reverse ordering", async () => {
            const result = await bPlusTree.list({ reverse: true });
            expect(result).to.have.lengthOf(6);
            expect(result[0].key).to.equal("f");
            expect(result[1].key).to.equal("e");
            expect(result[2].key).to.equal("d");
            expect(result[3].key).to.equal("c");
            expect(result[4].key).to.equal("b");
            expect(result[5].key).to.equal("a");
        });
        
        it("should combine range, limit, and reverse correctly", async () => {
            const result = await bPlusTree.list({ 
                gte: "b", 
                lt: "f", 
                limit: 2, 
                reverse: true 
            });
            
            expect(result).to.have.lengthOf(2);
            expect(result[0].key).to.equal("e");
            expect(result[1].key).to.equal("d");
        });
    });
    
    describe("Tree structure operations", () => {
        it("should handle node splitting for large datasets", async () => {
            // Insert enough key-value pairs to cause node splitting
            const data: Array<{key: string, value: string}> = [];
            for (let i = 0; i < 100; i++) {
                data.push({ key: `key${i.toString().padStart(3, '0')}`, value: `value${i}` });
            }
            
            await bPlusTree.write({ set: data });
            
            // Verify all values can be retrieved
            for (let i = 0; i < 100; i++) {
                const key = `key${i.toString().padStart(3, '0')}`;
                expect(await bPlusTree.get(key)).to.equal(`value${i}`);
            }
            
            // Test range query
            const result = await bPlusTree.list({ gte: "key050", lt: "key060" });
            expect(result).to.have.lengthOf(10);
            expect(result[0].key).to.equal("key050");
            expect(result[9].key).to.equal("key059");
        });
        
        it("should maintain tree balance after deletions", async () => {
            // Insert data
            const data: Array<{key: string, value: string}> = [];
            for (let i = 0; i < 100; i++) {
                data.push({ key: `key${i.toString().padStart(3, '0')}`, value: `value${i}` });
            }
            
            await bPlusTree.write({ set: data });
            
            // Delete some values
            const deleteKeys: string[] = [];
            for (let i = 20; i < 40; i++) {
                deleteKeys.push(`key${i.toString().padStart(3, '0')}`);
            }
            
            await bPlusTree.write({ delete: deleteKeys });
            
            // Verify deleted values are gone
            for (let i = 20; i < 40; i++) {
                const key = `key${i.toString().padStart(3, '0')}`;
                expect(await bPlusTree.get(key)).to.be.undefined;
            }
            
            // Verify remaining values are still accessible
            for (let i = 0; i < 20; i++) {
                const key = `key${i.toString().padStart(3, '0')}`;
                expect(await bPlusTree.get(key)).to.equal(`value${i}`);
            }
            
            for (let i = 40; i < 100; i++) {
                const key = `key${i.toString().padStart(3, '0')}`;
                expect(await bPlusTree.get(key)).to.equal(`value${i}`);
            }
            
            // Test range query over deletion gap
            const result = await bPlusTree.list({ gte: "key010", lt: "key050" });
            expect(result).to.have.lengthOf(30); // 50-10-20(deleted)=30
            expect(result[0].key).to.equal("key010");
            expect(result[9].key).to.equal("key019");
            expect(result[10].key).to.equal("key040");
        });
        
        it("should handle random inserts and deletes while maintaining correct order", async () => {
            const keys: string[] = [];
            for (let i = 0; i < 100; i++) {
                keys.push(`key${Math.floor(Math.random() * 1000).toString().padStart(4, '0')}`);
            }
            
            // Insert in random order
            for (let i = 0; i < keys.length; i++) {
                await bPlusTree.set(keys[i], `value-${keys[i]}`);
            }
            
            // Test that all keys were inserted
            for (let i = 0; i < keys.length; i++) {
                expect(await bPlusTree.get(keys[i])).to.equal(`value-${keys[i]}`);
            }
            
            // Get all values with list and ensure they're in order
            const allValues = await bPlusTree.list();
            for (let i = 1; i < allValues.length; i++) {
                expect(allValues[i-1].key < allValues[i].key).to.be.true;
            }
            
            // Delete random half of the keys
            const keysToDelete = keys.slice(0, 50);
            for (let i = 0; i < keysToDelete.length; i++) {
                await bPlusTree.delete(keysToDelete[i]);
            }
            
            // Ensure deleted keys are gone
            for (let i = 0; i < keysToDelete.length; i++) {
                expect(await bPlusTree.get(keysToDelete[i])).to.be.undefined;
            }
            
            // Ensure remaining keys are still there
            const remainingKeys = keys.slice(50);
            for (let i = 0; i < remainingKeys.length; i++) {
                expect(await bPlusTree.get(remainingKeys[i])).to.equal(`value-${remainingKeys[i]}`);
            }
            
            // Get all values again with list and ensure they're still in order
            const remainingValues = await bPlusTree.list();
            for (let i = 1; i < remainingValues.length; i++) {
                expect(remainingValues[i-1].key < remainingValues[i].key).to.be.true;
            }
        });
    });
    
    describe("Persistence", () => {
        it("should persist metadata between instances", async () => {
            // Insert data to cause B+ tree structure to form
            const data: Array<{key: string, value: string}> = [];
            for (let i = 0; i < 100; i++) {
                data.push({ key: `key${i.toString().padStart(3, '0')}`, value: `value${i}` });
            }
            
            await bPlusTree.write({ set: data });
            
            // Create a new bPlusTree with the same store
            const newBPlusTree = new AsyncBPlusTree(store);
            await newBPlusTree.init();
            
            // Verify the new tree can access the data
            for (let i = 0; i < 100; i++) {
                const key = `key${i.toString().padStart(3, '0')}`;
                expect(await newBPlusTree.get(key)).to.equal(`value${i}`);
            }
            
            // Test range query on the new instance
            const result = await newBPlusTree.list({ gte: "key050", lt: "key060" });
            expect(result).to.have.lengthOf(10);
            expect(result[0].key).to.equal("key050");
            expect(result[9].key).to.equal("key059");
        });
    });
}); 