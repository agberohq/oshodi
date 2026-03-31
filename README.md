# Oshodi

A high-performance embedded key-value store for Go, designed to work seamlessly with [Agbero](https://github.com/agberohq/agbero) load balancer and [Keeper](https://github.com/agberohq/keeper).

## Features

- **Blazing fast**: Lock-free architecture with sharded writes and zero-allocation paths
- **Embedded**: No external dependencies, runs in-process
- **Persistence**: Memory-mapped files with optional WAL
- **Buckets**: Namespaced collections for data isolation
- **Pub/Sub**: Built-in message passing between components
- **Analytics**: HyperLogLog cardinality estimation and Count-Min Sketch frequency tracking
- **Compression**: Optional zstd compression for large values
- **Concurrent**: Optimized for high concurrency with sharded maps

## Quick Start

```go
package main

import (
    "fmt"
    "log"
    
    "github.com/agberohq/oshodi"
)

func main() {
    // Open or create a database
    db, err := oshodi.Open("data.db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Get a bucket
    users := db.Bucket("users")
    
    // Store data
    err = users.Set([]byte("alice"), []byte(`{"name":"Alice","email":"alice@example.com"}`))
    if err != nil {
        log.Fatal(err)
    }
    
    // Retrieve data
    value, err := users.Get([]byte("alice"))
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Println(string(value))
    
    // Scan all keys
    users.Scan(func(key, value []byte) bool {
        fmt.Printf("%s: %s\n", key, value)
        return true // continue scanning
    })
}
```

## Configuration

```go
db, err := oshodi.Open("data.db",
    oshodi.WithShardCount(128),
    oshodi.WithCacheSize(10000),
    oshodi.WithCompression(4096, 5),  // compress values >4KB
    oshodi.WithBloomFilter(1_000_000, 0.01),
    oshodi.WithCardinality(14),        // enable key count estimation
)
```

## Pub/Sub Example

```go
// Subscribe to a channel
unsub := db.Subscribe("events", func(payload []byte) {
    fmt.Printf("Received: %s\n", payload)
})
defer unsub()

// Publish to a channel
db.Publish("events", []byte("hello world"))
```

## Bucket-Scoped Operations

```go
bucket := db.Bucket("mybucket")

// Register a handler
bucket.RegisterHandler("echo", func(req []byte) ([]byte, error) {
    return req, nil
})

// Call the handler
response, _ := bucket.Call("echo", []byte("ping"))
```

## Performance

Oshodi is optimized for:
- **Low latency**: Lock-free data structures throughout
- **High throughput**: Sharded writers, batch operations
- **Memory efficiency**: Memory-mapped files, configurable cache
- **Almost Zero allocations**: Hot paths avoid heap allocations

## License

MIT

## Author

Agbero HQ
