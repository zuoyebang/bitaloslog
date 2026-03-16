![bitalos](./docs/bitalos.png)

## 简介

- **高性能日志引擎**，专为日志存储设计，重点解决LSM-Tree架构下的日志写入与回收瓶颈。作为[bitalostored](https://github.com/zuoyebang/bitalostored)的raft日志核心组件。

## 主创

- **作者**：徐锐波（hustxurb@163.com），2018年12月加入作业帮，在职至今，先后负责直播课中台研发及作业帮平台研发；同时带领存储技术团队，从0到1研发Bitalos系列存储系统

- **贡献者**：幸福（wzxingfu@gmail.com）

## 关键技术

- **极致写入性能**：基于bitalostree的日志索引，支持高吞吐量的写操作，并消除传统日志系统频繁删除造成的IO放大.

- **高性能压缩索引技术**：bitalostree，基于超大page的b+ tree，创造性的索引压缩技术，消除b+ tree的写放大，并将读性能发挥到极致.

## 文档

- 技术架构和文档，参考官网：bitalos.zuoyebang.com