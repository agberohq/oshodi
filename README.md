# Oshodi

A high-performance embedded key-value store for Go, designed to work seamlessly with [Agbero](https://github.com/agberohq/agbero) load balancer, for specific needs but its has been more useful than its intended use.

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

## Vector Search

Oshodi ships an approximate nearest neighbor index (HNSW) on top of the
same storage engine. Embeddings are ordinary records; the graph is
rebuilt on open.

```go
import "github.com/agberohq/oshodi/ovector"

idx, err := ovector.Open(db)
if err != nil {
    log.Fatal(err)
}
defer idx.Close()

idx.Add("face:1", ovector.Vec{0.1, 0.2, 0.3})

// nearest neighbors by cosine distance
hits, _ := idx.Search([]float32{0.1, 0.2, 0.3}, 5)

// with distances
res, _ := idx.SearchWithDistance([]float32{0.1, 0.2, 0.3}, 5)
```

Cosine distance by default, Euclidean available. One index handle per
database; corrupt records are skipped and counted in `Stats`.

## Graph Traversals

Oshodi ships a property graph layer on top of the same storage engine.
Vertices and edges are ordinary records; adjacency is rebuilt on open.

```go
import "github.com/agberohq/oshodi/ograph"

g, err := ograph.Open(db)
if err != nil {
    log.Fatal(err)
}
defer g.Close()

g.AddVertex("artist:1", "artist", []byte(`{"name":"drake"}`))
g.AddVertex("song:1", "song", []byte(`{"name":"hotline bling"}`))
g.AddEdge("artist:1", "song:1", "performs", nil)

// traverse: two hops, filter by property, deterministic order
n, _ := g.V("artist:1").Out("performs").Has("name", "hotline bling").Count()

// algorithms: PageRank over every vertex, BFS distances
pr, _ := g.PageRank(0.85, 20)
dist, _ := g.BFS("artist:1", ograph.Outgoing)

// user-defined map over the element stream (never writes)
ids, _ := g.V("Artist:1").MutateID(strings.ToLower).Out("performs").IDs()
```

Traversal semantics: live, weakly consistent views (a mutation may land
between two stages); algorithms like PageRank hold a stable topology for
the whole computation. One graph handle per database.

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
