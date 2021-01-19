# Edge Databank

Databank is a simple object storage frontend for your data backend.

## Introduction

[databank.Databank](./databank.go) is a simple object storage frontend that can be configured with any data storage backend implementing the [databank.Driver](./databank.go) interface.

It provides additional functionality around storage transactions and queries, including lifetimes and cache invalidation, while the driver simply focuses on reading and writing.

<!-- TODO -->
<!-- databank.Multibank... -->

Some standard drivers are included:

- [atomic.Driver](./pkg/atomic/atomic.go) provides atomic object storage in memory
<!-- TODO -->
<!-- - [disk.Driver](./pkg/atomic/disk.go) provides persistent storage on the filesystem -->

## Usage

The simplest way to understand Databank usage is to look at the tests;

- [atomic_test.go](./pkg/atomic/atomic_test.go)
<!-- TODO -->
<!-- - [disk_test.go](./pkg/disk/disk_test.go) -->
