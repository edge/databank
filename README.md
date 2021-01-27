# Edge Databank

Databank is a simple object storage frontend for your data backend.

## Introduction

[databank.Databank](./databank.go) is a simple object storage frontend that can be configured with any data storage backend implementing the [databank.Driver](./databank.go) interface.

It provides additional functionality around storage transactions and queries, including lifetimes and cache invalidation, while the driver simply focuses on reading and writing.

Some standard drivers are included:

- [atomic.Driver](./pkg/atomic/atomic.go) provides atomic object storage in memory
- [disk.Driver](./pkg/atomic/disk.go) provides persistent storage on the filesystem

Some exotic drivers are also included:

- [proxy.SyncDriver](./pkg/proxy/sync.go) provides synchronised backend storage using multiple other drivers

## Usage

The simplest way to understand Databank usage is to look at the tests;

- [atomic_test.go](./pkg/atomic/atomic_test.go)
- [disk_test.go](./pkg/disk/disk_test.go)
- [sync_test.go](./pkg/disk/sync_test.go)

## Roadmap

Documentation will be updated massively once this package is stable. We promise!
