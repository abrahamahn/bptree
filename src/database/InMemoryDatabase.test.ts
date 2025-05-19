import { describe, it } from 'mocha';
import { expect } from 'chai';
import { InMemoryDatabase } from './InMemoryDatabase';

describe('InMemoryDatabase', () => {
    let db: InMemoryDatabase<string, string>;
    
    beforeEach(() => {
        db = new InMemoryDatabase<string, string>();
    });
    
    describe('Basic CRUD operations', () => {
        it('should set and get values correctly', () => {
            db.set('key1', 'value1');
            db.set('key2', 'value2');
            db.set('key3', 'value3');
            
            expect(db.get('key1')).to.equal('value1');
            expect(db.get('key2')).to.equal('value2');
            expect(db.get('key3')).to.equal('value3');
            expect(db.get('nonexistent')).to.be.undefined;
        });
        
        it('should update values correctly', () => {
            db.set('key1', 'value1');
            expect(db.get('key1')).to.equal('value1');
            
            db.set('key1', 'updated');
            expect(db.get('key1')).to.equal('updated');
        });
        
        it('should delete values correctly', () => {
            db.set('key1', 'value1');
            db.set('key2', 'value2');
            
            expect(db.get('key1')).to.equal('value1');
            db.delete('key1');
            expect(db.get('key1')).to.be.undefined;
            expect(db.get('key2')).to.equal('value2');
        });
        
        it('should handle batch operations', () => {
            db.write({
                set: [
                    { key: 'key1', value: 'value1' },
                    { key: 'key2', value: 'value2' }
                ]
            });
            
            expect(db.get('key1')).to.equal('value1');
            expect(db.get('key2')).to.equal('value2');
            
            db.write({
                set: [{ key: 'key3', value: 'value3' }],
                delete: ['key1']
            });
            
            expect(db.get('key1')).to.be.undefined;
            expect(db.get('key2')).to.equal('value2');
            expect(db.get('key3')).to.equal('value3');
        });
    });
    
    describe('Range queries', () => {
        beforeEach(() => {
            // Set up some ordered data
            const data: Array<{key: string, value: string}> = [
                { key: 'a', value: 'value-a' },
                { key: 'b', value: 'value-b' },
                { key: 'c', value: 'value-c' },
                { key: 'd', value: 'value-d' },
                { key: 'e', value: 'value-e' }
            ];
            
            db.write({ set: data });
        });
        
        it('should return all elements with no constraints', () => {
            const result = db.list();
            expect(result).to.have.lengthOf(5);
            expect(result[0].key).to.equal('a');
            expect(result[4].key).to.equal('e');
        });
        
        it('should handle gt/gte constraints', () => {
            let result = db.list({ gt: 'b' });
            expect(result).to.have.lengthOf(3);
            expect(result[0].key).to.equal('c');
            
            result = db.list({ gte: 'b' });
            expect(result).to.have.lengthOf(4);
            expect(result[0].key).to.equal('b');
        });
        
        it('should handle lt/lte constraints', () => {
            let result = db.list({ lt: 'd' });
            expect(result).to.have.lengthOf(3);
            expect(result[0].key).to.equal('a');
            expect(result[2].key).to.equal('c');
            
            result = db.list({ lte: 'd' });
            expect(result).to.have.lengthOf(4);
            expect(result[3].key).to.equal('d');
        });
        
        it('should handle combined constraints', () => {
            const result = db.list({ gt: 'b', lt: 'e' });
            expect(result).to.have.lengthOf(2);
            expect(result[0].key).to.equal('c');
            expect(result[1].key).to.equal('d');
        });
        
        it('should handle limit', () => {
            const result = db.list({ limit: 2 });
            expect(result).to.have.lengthOf(2);
            expect(result[0].key).to.equal('a');
            expect(result[1].key).to.equal('b');
        });
        
        it('should handle reverse', () => {
            const result = db.list({ reverse: true });
            expect(result).to.have.lengthOf(5);
            expect(result[0].key).to.equal('e');
            expect(result[4].key).to.equal('a');
        });
        
        it('should handle offset', () => {
            const result = db.list({ offset: 2 });
            expect(result).to.have.lengthOf(3);
            expect(result[0].key).to.equal('c');
            expect(result[2].key).to.equal('e');
        });
        
        it('should handle combined options', () => {
            // Recreate the test data to ensure we have a clean state
            db = new InMemoryDatabase<string, string>();
            
            const testData: Array<{key: string, value: string}> = [
                { key: 'a', value: 'value-a' },
                { key: 'b', value: 'value-b' },
                { key: 'c', value: 'value-c' },
                { key: 'd', value: 'value-d' },
                { key: 'e', value: 'value-e' }
            ];
            
            db.write({ set: testData });
            
            const result = db.list({ 
                gt: 'a',
                lt: 'e',
                limit: 2,
                offset: 0,
                reverse: true
            });
            
            // When reverse is true with bounds gt:'a' and lt:'e', 
            // we have [b,c,d] in reverse = [d,c,b] and limit 2 gives [d,c]
            expect(result).to.have.lengthOf(2);
            expect(result[0].key).to.equal('d');
            expect(result[1].key).to.equal('c');
        });
        
        it('should return empty array for invalid bounds', () => {
            let result = db.list({ gt: 'z' });
            expect(result).to.be.an('array').that.is.empty;
            
            result = db.list({ gt: 'e', lt: 'a' });
            expect(result).to.be.an('array').that.is.empty;
        });
    });
    
    describe('Custom comparator', () => {
        it('should use custom comparator for ordering', () => {
            // Create a database with reverse string ordering
            const reverseDb = new InMemoryDatabase<string, string>((a, b) => {
                if (a > b) return -1;
                if (a < b) return 1;
                return 0;
            });
            
            // Insert data
            reverseDb.write({
                set: [
                    { key: 'a', value: 'value-a' },
                    { key: 'b', value: 'value-b' },
                    { key: 'c', value: 'value-c' }
                ]
            });
            
            // Verify the order is reversed
            const result = reverseDb.list();
            expect(result).to.have.lengthOf(3);
            expect(result[0].key).to.equal('c');
            expect(result[1].key).to.equal('b');
            expect(result[2].key).to.equal('a');
        });
    });
}); 