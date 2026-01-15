package main

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"go.etcd.io/bbolt"
)

/* 运行测试 */
// go test -bench=. -benchmem -count=1

// --- 1. 统一的存储引擎接口 ---

type StorageEngine interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Scan(prefix []byte) (map[string][]byte, error)
	Close() error
}

// --- 2. BoltDB (bbolt) 引擎实现 ---

type BoltDBEngine struct {
	db     *bbolt.DB
	bucket []byte
}

func NewBoltDBEngine(path string, bucketName string) (StorageEngine, error) {
	db, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}
	bucket := []byte(bucketName)
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucket)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &BoltDBEngine{db: db, bucket: bucket}, nil
}

func (b *BoltDBEngine) Put(key, value []byte) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(b.bucket).Put(key, value)
	})
}

func (b *BoltDBEngine) Get(key []byte) ([]byte, error) {
	var val []byte
	err := b.db.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(b.bucket).Get(key)
		// Must copy the value, as it's only valid during the transaction.
		if v != nil {
			val = make([]byte, len(v))
			copy(val, v)
		}
		return nil
	})
	return val, err
}

func (b *BoltDBEngine) Scan(prefix []byte) (map[string][]byte, error) {
	results := make(map[string][]byte)
	err := b.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(b.bucket).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			// Must copy key and value
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			valCopy := make([]byte, len(v))
			copy(valCopy, v)
			results[string(keyCopy)] = valCopy
		}
		return nil
	})
	return results, err
}

func (b *BoltDBEngine) Close() error {
	return b.db.Close()
}

// --- 3. Pebble 引擎实现 ---

// 定义一个共享的缓存大小
const cacheSizeBytes = 128 * 1024 * 1024 // 128 MB

type PebbleEngine struct {
	db    *pebble.DB
	cache *pebble.Cache // 添加 cache 字段以便关闭
}

func NewPebbleEngine(path string) (StorageEngine, error) {
	// 1. 创建一个新的 Cache 实例
	cache := pebble.NewCache(cacheSizeBytes)

	// 2. 在 pebble.Options 中设置 Cache
	opts := &pebble.Options{
		Cache: cache,
		// 其他你可能想调优的选项...
	}
	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}
	return &PebbleEngine{db: db}, nil
}

func (p *PebbleEngine) Put(key, value []byte) error {
	// Use WriteOptions.Sync to make it an apples-to-apples comparison with bbolt's transaction commit
	return p.db.Set(key, value, pebble.Sync)
}

func (p *PebbleEngine) Get(key []byte) ([]byte, error) {
	val, closer, err := p.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	// Must copy the value
	valCopy := make([]byte, len(val))
	copy(valCopy, val)
	return valCopy, nil
}

func (p *PebbleEngine) Scan(prefix []byte) (map[string][]byte, error) {
	results := make(map[string][]byte)
	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid() && bytes.HasPrefix(iter.Key(), prefix); iter.Next() {
		// Must copy key and value
		keyCopy := make([]byte, len(iter.Key()))
		copy(keyCopy, iter.Key())
		valCopy := make([]byte, len(iter.Value()))
		copy(valCopy, iter.Value())
		results[string(keyCopy)] = valCopy
	}
	return results, nil
}

func (p *PebbleEngine) Close() error {
	return p.db.Close()
}

// --- 4. Benchmark Helper & Main Function ---

const (
	InitialObjectCount = 5000
	PayloadSizeBytes   = 1024 * 16 // 16 KB, simulating a simple Pod object
)

var podKeyPrefix = []byte("/registry/pods/default/")

func generateKV(id int) ([]byte, []byte) {
	key := fmt.Sprintf("%s%s-%d", podKeyPrefix, "pod", id)
	// Create a random-ish payload of the desired size
	value := make([]byte, PayloadSizeBytes)
	rand.Read(value)
	return []byte(key), value
}

// setupDB creates a temporary DB and pre-populates it with data
func setupDB(b *testing.B, newEngine func(path string) (StorageEngine, error)) StorageEngine {
	dir, err := os.MkdirTemp("", "benchmark_db")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}

	// Use b.Cleanup to ensure the DB is closed and dir is removed after the benchmark
	b.Cleanup(func() {
		os.RemoveAll(dir)
	})

	engine, err := newEngine(filepath.Join(dir, "db"))
	if err != nil {
		b.Fatalf("failed to create engine: %v", err)
	}
	b.Cleanup(func() {
		if err := engine.Close(); err != nil {
			log.Printf("failed to close engine: %v", err)
		}
	})

	// Pre-populate the database
	for i := range InitialObjectCount {
		key, value := generateKV(i)
		if err := engine.Put(key, value); err != nil {
			b.Fatalf("failed to pre-populate db: %v", err)
		}
	}
	return engine
}

func BenchmarkStorageEngines(b *testing.B) {
	// Map of engine constructors
	engines := map[string]func(path string) (StorageEngine, error){
		"bbolt": func(path string) (StorageEngine, error) {
			return NewBoltDBEngine(path, "etcd")
		},
		"pebble": func(path string) (StorageEngine, error) {
			return NewPebbleEngine(path)
		},
	}

	for name, newEngine := range engines {
		// Run a sub-benchmark for each engine
		b.Run(name, func(b *testing.B) {
			engine := setupDB(b, newEngine)

			// --- SCENARIO 1: Write-Heavy (Cluster Bootstrap) ---
			b.Run("WriteHeavy_ClusterBootstrap", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer() // Start timer after setup

				for i := 0; i < b.N; i++ {
					// Write a new, unique key in each iteration
					key, value := generateKV(InitialObjectCount + i)
					if err := engine.Put(key, value); err != nil {
						b.Fatal(err)
					}
				}
			})

			// --- SCENARIO 2: Read-Heavy (Controller Loop) ---
			b.Run("ReadHeavy_ControllerLoop", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					if _, err := engine.Scan(podKeyPrefix); err != nil {
						b.Fatal(err)
					}
				}
			})

			// --- SCENARIO 3: Mixed Read-Write (Node Status Updates) ---
			b.Run("Mixed_NodeStatusUpdates", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()

				// Use RunParallel to simulate concurrent Kubelets/Controllers
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						// Pick a random existing key
						id := rand.Intn(InitialObjectCount)
						key, _ := generateKV(id)

						// Read
						value, err := engine.Get(key)
						if err != nil {
							b.Error(err)
							continue
						}

						// Modify (optional, here we just re-write the same value)
						// In a real scenario, you'd deserialize, change a field, and serialize.

						// Write
						if err := engine.Put(key, value); err != nil {
							b.Error(err)
							continue
						}
					}
				})
			})
			// --- SCENARIO 4: Write Churn (Sequential Writes with Updates) ---
			b.Run("WriteChurn_SequentialWithUpdates", func(b *testing.B) {
				engine := setupDB(b, newEngine)

				// 使用 atomic counter 来安全地生成新的顺序 key ID
				var newKeyCounter int64 = InitialObjectCount

				b.ReportAllocs()
				b.ResetTimer()

				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						// 90% 的概率是写入新的顺序 key
						if rand.Intn(100) < 90 {
							newID := atomic.AddInt64(&newKeyCounter, 1)
							key, value := generateKV(int(newID))
							if err := engine.Put(key, value); err != nil {
								b.Error(err)
							}
						} else {
							// 10% 的概率是更新一个已存在的旧 key
							idToUpdate := rand.Intn(InitialObjectCount)
							key, value := generateKV(idToUpdate) // 生成新的 value 来模拟更新
							if err := engine.Put(key, value); err != nil {
								b.Error(err)
							}
						}
					}
				})
			})

			// --- SCENARIO 5: Read-Random (Single Object Get) ---
			b.Run("ReadRandom_SingleObjectGet", func(b *testing.B) {
				engine := setupDB(b, newEngine)
				b.ReportAllocs()
				b.ResetTimer()

				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						// Pick a random existing key
						id := rand.Intn(InitialObjectCount)
						key, _ := generateKV(id)

						// Read
						value, err := engine.Get(key)
						if err != nil {
							b.Error(err)
						}
						// A simple check to ensure the read is not optimized away
						if value == nil {
							b.Error("Get returned nil value unexpectedly")
						}
					}
				})
			})
		})
	}
}
