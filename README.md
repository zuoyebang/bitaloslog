![bitalos](./docs/bitalos.png)

## Introduction

- **High-performance Log Engine**, specifically designed for log storage, focuses on addressing the bottlenecks of log writing and recycling in the LSM-Tree architecture. It serves as the core component of the raft log in [bitalostored](https://github.com/zuoyebang/bitalostored).

## Main Creator

- **Author**：Xu Ruibo(hustxurb@163.com), joined Zuoyebang in December 2018 (working till now), is responsible for live class middle platform and Zuoyebang platform, and leads the storage technology team to develop Bitalos from 0 to 1.

- **Contributor**: Xingfu (wzxingfu@gmail.com)

## Key Technologie

- **Ultimate Write Performance**: Based on bitalostree log indexing, it supports high-throughput write operations and eliminates the IO amplification caused by frequent deletions in traditional log systems.

- **High-performance Compression Indexing Technology**: bitalostree, a B+ tree based on super-large pages, features a creative indexing compression technique that eliminates write amplification in B+ trees and maximizes read performance.

## Document

- For technical architecture and documentation, please refer to the official website: bitalos.zuoyebang.com