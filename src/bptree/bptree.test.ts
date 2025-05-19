import { describe, it, beforeEach } from 'mocha';
import { expect } from "chai";
import { BPlusTree } from "./bptree";
import { InMemoryDatabase } from "../database/InMemoryDatabase";

describe("BPlusTree with InMemoryDatabase", () => {
    let bPlusTree: BPlusTree;
    let store: InMemoryDatabase<string, string>;
    
    beforeEach(() => {
        // Use InMemoryDatabase as the underlying storage
        store = new InMemoryDatabase<string, string>();
        bPlusTree = new BPlusTree(store);
    });
    
    describe("Basic CRUD operations", () => {
        it("should set and get values correctly", () => {
            bPlusTree.set("key1", "value1");
            bPlusTree.set("key2", "value2");
            bPlusTree.set("key3", "value3");
            
            expect(bPlusTree.get("key1")).to.equal("value1");
            expect(bPlusTree.get("key2")).to.equal("value2");
            expect(bPlusTree.get("key3")).to.equal("value3");
            expect(bPlusTree.get("keyNotFound")).to.be.undefined;
        });
        
        it("should update values correctly", () => {
            bPlusTree.set("key1", "value1");
            expect(bPlusTree.get("key1")).to.equal("value1");
            
            bPlusTree.set("key1", "updatedValue");
            expect(bPlusTree.get("key1")).to.equal("updatedValue");
        });
        
        it("should delete values correctly", () => {
            bPlusTree.set("key1", "value1");
            bPlusTree.set("key2", "value2");
            
            expect(bPlusTree.get("key1")).to.equal("value1");
            expect(bPlusTree.get("key2")).to.equal("value2");
            
            bPlusTree.delete("key1");
            
            expect(bPlusTree.get("key1")).to.be.undefined;
            expect(bPlusTree.get("key2")).to.equal("value2");
        });
        
        it("should handle batch operations with write", () => {
            bPlusTree.write({
                set: [
                    { key: "key1", value: "value1" },
                    { key: "key2", value: "value2" },
                    { key: "key3", value: "value3" }
                ]
            });
            
            expect(bPlusTree.get("key1")).to.equal("value1");
            expect(bPlusTree.get("key2")).to.equal("value2");
            expect(bPlusTree.get("key3")).to.equal("value3");
            
            bPlusTree.write({
                set: [{ key: "key4", value: "value4" }],
                delete: ["key1", "key3"]
            });
            
            expect(bPlusTree.get("key1")).to.be.undefined;
            expect(bPlusTree.get("key2")).to.equal("value2");
            expect(bPlusTree.get("key3")).to.be.undefined;
            expect(bPlusTree.get("key4")).to.equal("value4");
        });
    });
    
    describe("Range queries", () => {
        beforeEach(() => {
            // Insert values in non-sorted order
            const data = [
                { key: "e", value: "valueE" },
                { key: "c", value: "valueC" },
                { key: "a", value: "valueA" },
                { key: "d", value: "valueD" },
                { key: "b", value: "valueB" },
                { key: "f", value: "valueF" },
            ];
            
            bPlusTree.write({ set: data });
        });
        
        it("should return all elements when no range is specified", () => {
            const result = bPlusTree.list();
            expect(result).to.have.lengthOf(6);
            expect(result[0].key).to.equal("a");
            expect(result[1].key).to.equal("b");
            expect(result[2].key).to.equal("c");
            expect(result[3].key).to.equal("d");
            expect(result[4].key).to.equal("e");
            expect(result[5].key).to.equal("f");
        });
        
        it("should handle gt/gte bounds correctly", () => {
            let result = bPlusTree.list({ gt: "c" });
            expect(result).to.have.lengthOf(3);
            expect(result[0].key).to.equal("d");
            expect(result[2].key).to.equal("f");
            
            result = bPlusTree.list({ gte: "c" });
            expect(result).to.have.lengthOf(4);
            expect(result[0].key).to.equal("c");
            expect(result[3].key).to.equal("f");
        });
        
        it("should handle lt/lte bounds correctly", () => {
            let result = bPlusTree.list({ lt: "d" });
            expect(result).to.have.lengthOf(3);
            expect(result[0].key).to.equal("a");
            expect(result[2].key).to.equal("c");
            
            result = bPlusTree.list({ lte: "d" });
            expect(result).to.have.lengthOf(4);
            expect(result[0].key).to.equal("a");
            expect(result[3].key).to.equal("d");
        });
        
        it("should handle combined range bounds correctly", () => {
            let result = bPlusTree.list({ gt: "b", lt: "e" });
            expect(result).to.have.lengthOf(2);
            expect(result[0].key).to.equal("c");
            expect(result[1].key).to.equal("d");
            
            result = bPlusTree.list({ gte: "b", lte: "e" });
            expect(result).to.have.lengthOf(4);
            expect(result[0].key).to.equal("b");
            expect(result[3].key).to.equal("e");
        });
        
        it("should limit results correctly", () => {
            const result = bPlusTree.list({ limit: 3 });
            expect(result).to.have.lengthOf(3);
            expect(result[0].key).to.equal("a");
            expect(result[1].key).to.equal("b");
            expect(result[2].key).to.equal("c");
        });
        
        it("should handle reverse ordering", () => {
            const result = bPlusTree.list({ reverse: true });
            expect(result).to.have.lengthOf(6);
            expect(result[0].key).to.equal("f");
            expect(result[1].key).to.equal("e");
            expect(result[2].key).to.equal("d");
            expect(result[3].key).to.equal("c");
            expect(result[4].key).to.equal("b");
            expect(result[5].key).to.equal("a");
        });
        
        it("should combine range, limit, and reverse correctly", () => {
            const result = bPlusTree.list({ 
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
        it("should handle node splitting for large datasets", () => {
            // Insert enough key-value pairs to cause node splitting
            const data: Array<{key: string, value: string}> = [];
            for (let i = 0; i < 100; i++) {
                data.push({ key: `key${i.toString().padStart(3, '0')}`, value: `value${i}` });
            }
            
            bPlusTree.write({ set: data });
            
            // Verify all values can be retrieved
            for (let i = 0; i < 100; i++) {
                const key = `key${i.toString().padStart(3, '0')}`;
                expect(bPlusTree.get(key)).to.equal(`value${i}`);
            }
            
            // Test range query
            const result = bPlusTree.list({ gte: "key050", lt: "key060" });
            expect(result).to.have.lengthOf(10);
            expect(result[0].key).to.equal("key050");
            expect(result[9].key).to.equal("key059");
        });
        
        it("should maintain tree balance after deletions", () => {
            // Insert data
            const data: Array<{key: string, value: string}> = [];
            for (let i = 0; i < 100; i++) {
                data.push({ key: `key${i.toString().padStart(3, '0')}`, value: `value${i}` });
            }
            
            bPlusTree.write({ set: data });
            
            // Delete some values
            const deleteKeys: string[] = [];
            for (let i = 20; i < 40; i++) {
                deleteKeys.push(`key${i.toString().padStart(3, '0')}`);
            }
            
            bPlusTree.write({ delete: deleteKeys });
            
            // Verify deleted values are gone
            for (let i = 20; i < 40; i++) {
                const key = `key${i.toString().padStart(3, '0')}`;
                expect(bPlusTree.get(key)).to.be.undefined;
            }
            
            // Verify remaining values are still accessible
            for (let i = 0; i < 20; i++) {
                const key = `key${i.toString().padStart(3, '0')}`;
                expect(bPlusTree.get(key)).to.equal(`value${i}`);
            }
            
            for (let i = 40; i < 100; i++) {
                const key = `key${i.toString().padStart(3, '0')}`;
                expect(bPlusTree.get(key)).to.equal(`value${i}`);
            }
            
            // Test range query over deletion gap
            const result = bPlusTree.list({ gte: "key010", lt: "key050" });
            expect(result).to.have.lengthOf(30); // 50-10-20(deleted)=30
            expect(result[0].key).to.equal("key010");
            expect(result[9].key).to.equal("key019");
            expect(result[10].key).to.equal("key040");
        });
        
        it("should handle random inserts and deletes while maintaining correct order", () => {
            const keys: string[] = [];
            for (let i = 0; i < 100; i++) {
                keys.push(`key${Math.floor(Math.random() * 1000).toString().padStart(4, '0')}`);
            }
            
            // Insert in random order
            for (let i = 0; i < keys.length; i++) {
                bPlusTree.set(keys[i], `value-${keys[i]}`);
            }
            
            // Test that all keys were inserted
            for (let i = 0; i < keys.length; i++) {
                expect(bPlusTree.get(keys[i])).to.equal(`value-${keys[i]}`);
            }
            
            // Get all values with list and ensure they're in order
            const allValues = bPlusTree.list();
            for (let i = 1; i < allValues.length; i++) {
                expect(allValues[i-1].key < allValues[i].key).to.be.true;
            }
            
            // Delete random half of the keys
            const keysToDelete = keys.slice(0, 50);
            for (let i = 0; i < keysToDelete.length; i++) {
                bPlusTree.delete(keysToDelete[i]);
            }
            
            // Ensure deleted keys are gone
            for (let i = 0; i < keysToDelete.length; i++) {
                expect(bPlusTree.get(keysToDelete[i])).to.be.undefined;
            }
            
            // Ensure remaining keys are still there
            const remainingKeys = keys.slice(50);
            for (let i = 0; i < remainingKeys.length; i++) {
                expect(bPlusTree.get(remainingKeys[i])).to.equal(`value-${remainingKeys[i]}`);
            }
            
            // Get all values again with list and ensure they're still in order
            const remainingValues = bPlusTree.list();
            for (let i = 1; i < remainingValues.length; i++) {
                expect(remainingValues[i-1].key < remainingValues[i].key).to.be.true;
            }
        });
    });
    
    describe("Persistence", () => {
        it("should persist metadata between instances", () => {
            // Insert data to cause B+ tree structure to form
            const data: Array<{key: string, value: string}> = [];
            for (let i = 0; i < 100; i++) {
                data.push({ key: `key${i.toString().padStart(3, '0')}`, value: `value${i}` });
            }
            
            bPlusTree.write({ set: data });
            
            // Create a new bPlusTree with the same store
            const newBPlusTree = new BPlusTree(store);
            
            // Verify the new tree can access the data
            for (let i = 0; i < 100; i++) {
                const key = `key${i.toString().padStart(3, '0')}`;
                expect(newBPlusTree.get(key)).to.equal(`value${i}`);
            }
            
            // Test range query on the new instance
            const result = newBPlusTree.list({ gte: "key050", lt: "key060" });
            expect(result).to.have.lengthOf(10);
            expect(result[0].key).to.equal("key050");
            expect(result[9].key).to.equal("key059");
        });
    });
}); 