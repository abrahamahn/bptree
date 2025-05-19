import { InMemoryDatabase } from "../database/InMemoryDatabase";
import { performance } from "perf_hooks";

// Helper to generate sequential data
function generateSequentialData(count: number): { key: string; value: string }[] {
    const data: { key: string; value: string }[] = [];
    for (let i = 0; i < count; i++) {
        const key = `key-${i.toString().padStart(8, '0')}`;
        const value = `value-${i}`;
        data.push({ key, value });
    }
    return data;
}

// Helper to generate random data
function generateRandomData(count: number): { key: string; value: string }[] {
    const data: { key: string; value: string }[] = [];
    for (let i = 0; i < count; i++) {
        const randomNum = Math.floor(Math.random() * 1000000).toString().padStart(8, '0');
        const key = `key-${randomNum}`;
        const value = `value-${i}`;
        data.push({ key, value });
    }
    return data;
}

// Benchmark a function and return execution time in ms
async function benchmark<T>(fn: () => Promise<T> | T): Promise<number> {
    const start = performance.now();
    const result = fn();
    if (result instanceof Promise) {
        await result;
    }
    return performance.now() - start;
}

// Run a benchmark multiple times and return average
async function runBenchmark<T>(
    name: string, 
    fn: () => Promise<T> | T, 
    iterations: number = 5
): Promise<number> {
    console.log(`Running benchmark: ${name}`);
    let total = 0;
    const times: number[] = [];
    
    for (let i = 0; i < iterations; i++) {
        const time = await benchmark(fn);
        total += time;
        times.push(time);
        process.stdout.write('.');
    }
    
    const avg = total / iterations;
    const min = Math.min(...times);
    const max = Math.max(...times);
    
    console.log(`\n  Average: ${avg.toFixed(2)}ms, Min: ${min.toFixed(2)}ms, Max: ${max.toFixed(2)}ms`);
    return avg;
}

// Benchmark InMemoryDatabase operations with different data sizes
async function benchmarkInMemoryDatabase() {
    console.log("\n=========================================");
    console.log("IN-MEMORY DATABASE PERFORMANCE BENCHMARKS");
    console.log("=========================================\n");
    
    const dataSizes = [1000, 10000, 100000];
    
    for (const dataSize of dataSizes) {
        console.log(`\nData size: ${dataSize} items\n`);
        
        // Generate test data sets
        const sequentialData = generateSequentialData(dataSize);
        const randomData = generateRandomData(dataSize);
        
        // Benchmark sequential insert
        await runBenchmark(`Sequential Insert (${dataSize} items)`, () => {
            const db = new InMemoryDatabase<string, string>();
            db.write({ set: sequentialData });
            return db;
        });
        
        // Benchmark random insert
        await runBenchmark(`Random Insert (${dataSize} items)`, () => {
            const db = new InMemoryDatabase<string, string>();
            db.write({ set: randomData });
            return db;
        });
        
        // Setup a database for query testing
        const db = new InMemoryDatabase<string, string>();
        db.write({ set: sequentialData });
        
        // Benchmark point lookups
        await runBenchmark(`Point Lookups (100 random keys)`, () => {
            for (let i = 0; i < 100; i++) {
                const randomIndex = Math.floor(Math.random() * dataSize);
                const key = sequentialData[randomIndex].key;
                db.get(key);
            }
        });
        
        // Benchmark range queries with different sizes
        const ranges = [
            { title: "Small Range (10 items)", args: { gte: "key-00000100", lt: "key-00000110" } },
            { title: "Medium Range (100 items)", args: { gte: "key-00001000", lt: "key-00001100" } },
        ];
        
        if (dataSize >= 10000) {
            ranges.push({ title: "Large Range (1000 items)", args: { gte: "key-00002000", lt: "key-00003000" } });
        }
        
        for (const range of ranges) {
            await runBenchmark(`Range Query - ${range.title}`, () => {
                return db.list(range.args);
            });
        }
        
        // Benchmark list with options
        await runBenchmark(`List with Limit (100 items)`, () => {
            return db.list({ limit: 100 });
        });
        
        await runBenchmark(`List with Reverse`, () => {
            return db.list({ reverse: true, limit: 100 });
        });
        
        await runBenchmark(`List with Combined Options`, () => {
            return db.list({
                gte: "key-00001000",
                lt: "key-00002000",
                limit: 50,
                offset: 25,
                reverse: true
            });
        });
        
        // Benchmark deletes - random keys
        const keysToDel = sequentialData.slice(0, 1000).map(item => item.key);
        
        await runBenchmark(`Individual Deletes (100 items)`, () => {
            const tempDb = new InMemoryDatabase<string, string>();
            tempDb.write({ set: sequentialData });
            
            for (let i = 0; i < 100; i++) {
                tempDb.delete(keysToDel[i]);
            }
        });
        
        await runBenchmark(`Batch Delete (1000 items)`, () => {
            const tempDb = new InMemoryDatabase<string, string>();
            tempDb.write({ set: sequentialData });
            tempDb.write({ delete: keysToDel });
        });
    }
}

// Compare InMemoryDatabase to raw Maps
async function compareToRawMap() {
    console.log("\n=========================================");
    console.log("IN-MEMORY DATABASE VS JAVASCRIPT MAP");
    console.log("=========================================\n");
    
    const dataSize = 100000;
    const sequentialData = generateSequentialData(dataSize);
    
    // Test JavaScript Map
    await runBenchmark(`JavaScript Map - Insert ${dataSize} items`, () => {
        const map = new Map<string, string>();
        for (const item of sequentialData) {
            map.set(item.key, item.value);
        }
        return map;
    });
    
    // Test InMemoryDatabase
    await runBenchmark(`InMemoryDatabase - Insert ${dataSize} items`, () => {
        const db = new InMemoryDatabase<string, string>();
        db.write({ set: sequentialData });
        return db;
    });
    
    // Setup for lookup testing
    const map = new Map<string, string>();
    for (const item of sequentialData) {
        map.set(item.key, item.value);
    }
    
    const db = new InMemoryDatabase<string, string>();
    db.write({ set: sequentialData });
    
    // Test lookups
    await runBenchmark(`JavaScript Map - Random Lookups (1000)`, () => {
        for (let i = 0; i < 1000; i++) {
            const randomIndex = Math.floor(Math.random() * dataSize);
            const key = sequentialData[randomIndex].key;
            map.get(key);
        }
    });
    
    await runBenchmark(`InMemoryDatabase - Random Lookups (1000)`, () => {
        for (let i = 0; i < 1000; i++) {
            const randomIndex = Math.floor(Math.random() * dataSize);
            const key = sequentialData[randomIndex].key;
            db.get(key);
        }
    });
    
    // Test range queries (Map doesn't have this natively, so simulate it)
    const start = "key-00001000";
    const end = "key-00002000";
    
    await runBenchmark(`JavaScript Map - Range Scan (simulated)`, () => {
        const result: { key: string; value: string }[] = [];
        for (const [key, value] of map.entries()) {
            if (key >= start && key < end) {
                result.push({ key, value });
            }
        }
        return result;
    });
    
    await runBenchmark(`InMemoryDatabase - Range Scan`, () => {
        return db.list({ gte: start, lt: end });
    });
}

// Main benchmark runner
async function runBenchmarks() {
    console.log("Starting Performance Benchmarks...");
    
    await benchmarkInMemoryDatabase();
    await compareToRawMap();
    
    console.log("\nBenchmarks complete!");
}

// Run benchmarks
runBenchmarks().catch(console.error); 