import { describe, it, beforeEach } from 'mocha';
import { expect } from 'chai';
import { AsyncInMemoryDatabase } from "./AsyncInMemoryDatabase";

describe('AsyncInMemoryDatabase (AsyncOrderedKeyValueApi)', () => {
    let storage: AsyncInMemoryDatabase<string>;
    
    beforeEach(() => {
        storage = new AsyncInMemoryDatabase<string>();
    });
    
    it('should set and get values correctly', async () => {
        await storage.write({
            set: [
                { key: 'key1', value: 'value1' },
                { key: 'key2', value: 'value2' }
            ]
        });
        
        expect(await storage.get('key1')).to.equal('value1');
        expect(await storage.get('key2')).to.equal('value2');
        expect(await storage.get('nonexistent')).to.be.undefined;
    });
    
    it('should delete values correctly', async () => {
        await storage.write({
            set: [
                { key: 'key1', value: 'value1' },
                { key: 'key2', value: 'value2' }
            ]
        });
        
        await storage.write({
            delete: ['key1']
        });
        
        expect(await storage.get('key1')).to.be.undefined;
        expect(await storage.get('key2')).to.equal('value2');
    });
    
    it('should handle list operations with various constraints', async () => {
        // Set up test data
        await storage.write({
            set: [
                { key: 'a', value: 'value-a' },
                { key: 'b', value: 'value-b' },
                { key: 'c', value: 'value-c' },
                { key: 'd', value: 'value-d' },
                { key: 'e', value: 'value-e' }
            ]
        });
        
        // Test basic listing
        let results = await storage.list();
        expect(results).to.have.lengthOf(5);
        expect(results[0].key).to.equal('a');
        
        // Test with range constraints
        results = await storage.list({ gt: 'b', lt: 'e' });
        expect(results).to.have.lengthOf(2);
        expect(results[0].key).to.equal('c');
        expect(results[1].key).to.equal('d');
        
        // Test with limit
        results = await storage.list({ limit: 2 });
        expect(results).to.have.lengthOf(2);
        
        // Test with reverse
        results = await storage.list({ reverse: true });
        expect(results[0].key).to.equal('e');
    });
    
    it('should handle combining list constraints', async () => {
        // Set up test data
        await storage.write({
            set: [
                { key: 'a', value: 'value-a' },
                { key: 'b', value: 'value-b' },
                { key: 'c', value: 'value-c' },
                { key: 'd', value: 'value-d' },
                { key: 'e', value: 'value-e' }
            ]
        });
        
        const results = await storage.list({
            gt: 'a',
            lt: 'e',
            limit: 2,
            reverse: true
        });
        
        expect(results).to.have.lengthOf(2);
        expect(results[0].key).to.equal('d');
        expect(results[1].key).to.equal('c');
    });
});

// Note: To properly test the actual IndexedDbOrderedKeyValueStorage class,
// you would need to:
// 1. Use a library like 'fake-indexeddb' to mock IndexedDB in Node.js
// 2. Or run these tests in a browser environment where IndexedDB is available 