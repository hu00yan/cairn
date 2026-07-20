# Cairn 完整设计与集成计划审查

## 结论摘要

计划的主线是合理的：先冻结单节点不可变对象与 root 可见性，再建设故障模拟、复制、控制面、EC 和客户端集成。但当前计划过早把多个尚未冻结的语义绑定在一起，不能直接作为十个连续 coding 批次执行。

当前仓库已有 `cairn-core`、`cairn-device`、`cairn-ec`、`cairn-sim` 和空的 `cairn-model`；尚无真实 `FileDevice`、控制面、placement 或复制实现。因此下一步仍应先完成单节点 Gate 1 和进程内模拟 gate，而不是铺开真实集群 crate。

## 已确认的数学错误

Reed–Solomon 的 `k+m` 配置共有 `k+m` 个 shard，任意 `k` 个有效 shard 可以重建原数据，因此最多容忍 `m` 个 shard 丢失。官方 Rust API 也将 data shards 和 parity shards 作为两个独立参数；Ceph 文档明确说明 K+M 中任意 K 个 shard 足以恢复对象。

因此：

| 目标 | 正确配置 | 总 shard | 可丢失 | 原始容量开销 |
|---|---:|---:|---:|---:|
| 当前计划的 10+6 | 10 data + 6 parity | 16 | 6 | 1.6x |
| “10 份可以丢 4 份” | 6 data + 4 parity | 10 | 4 | 1.667x |
| “10 个 data shard 丢 4 个” | 10 data + 4 parity | 14 | 4 | 1.4x |

所以计划必须先明确“10”指总 shard 数还是 data shard 数。不能同时写成“10+6”和“总共 10 份可丢 4 份”。

来源：[reed-solomon-erasure Rust API](https://docs.rs/reed-solomon-erasure/latest/reed_solomon_erasure/struct.ReedSolomon.html)、[Ceph erasure-coding developer notes](https://docs.ceph.com/en/latest/dev/osd_internals/erasure_coding/developer_notes/)、[Backblaze Reed-Solomon说明](https://www.backblaze.com/blog/reed-solomon/)。

## 计划中的关键风险

1. **阶段边界过宽。** 单节点格式、恢复和 GC 尚未完成时，不能稳定定义复制、EC descriptor 和跨节点 root 提交。应先完成本地 crash matrix、差分模型和 FileDevice，再进入网络。

2. **failure domain 没有成为可验证的故障模型。** “16 个不同 host”不等于能承受 6 个独立故障。必须定义 host、rack、zone、供电域之间的独立性，并把 placement 的最坏故障集合写成可计算的 contract test。

3. **EC checksum 不能替代可信元数据。** checksum 能筛出错误 shard，但必须明确 descriptor/checksum 的持久化位置和信任根；如果 checksum 元数据与 shard 一起丢失或被篡改，解码器不能盲目把任意 10 份数据当成正确结果。

4. **复制 root 与本地 root 的语义尚未统一。** 计划同时有本地 generation、placement epoch、控制面 object state 和 namespace root CAS。需要先定义哪个状态是权威提交点，以及客户端收到超时后如何判断“已提交但响应丢失”。

5. **BlockDevice 契约存在接口漂移。** 计划中的 flush 使用 `&self`，当前 Cairn 实现使用 `&mut self`。这不是表面签名差异：它会改变并发访问、设备内部同步和单 writer 约束，必须在阶段二冻结后再扩散到 FileDevice/SimDisk。

6. **格式升级必须先于长期 API。** 当前实现已经采用 v2 和 chunk/manifest 域分离哈希；后续 `cairn-types`、`cairn-codec` 应承载这一规则，不能让公开 `Hasher` 产生与 Store 不一致的 ObjectId。

7. **“所有 16 shard durable 后才提交”是安全策略，不是可用性策略。** 它会让任一 shard 写不成功的对象保持 Preparing。计划需要明确重试、取消、超时和最终 tombstone，否则会产生无限 Preparing 对象。

8. **Property/model 测试应先于大规模回归测试。** 每个阶段应有纯模型和有限状态机测试：生成操作序列、crash point、节点故障集合，并在每个成功 commit 后比较可见逻辑状态。单个故障回归测试只用于锁定已发现的反例。

## 建议的重新排序

1. 冻结 `cairn-types`、`cairn-codec`、错误模型和 root/commit 语义；明确 EC 参数命名。
2. 完成 SimDisk 的 bounded fault schedule，并用性质测试枚举 write/flush/crash 边界。
3. 完成 record scanner、双 superblock、完整 generation 回退、FileDevice 和差分模型。
4. 完成 chunk/manifest 流式 API、GC 和 checkpoint；证明索引损坏不影响正确性。
5. 单独实现并验证 `cairn-ec`：先测 codec 的任意 `k` 有效 shard、checksum 排除和超过容错数失败。
6. 再实现 placement 与 failure-domain contract，随后才实现三副本和 repair。
7. 最后建设控制面、网络协议、统一事务和进程内集群测试。

## Gate 0：在继续扩展前必须补齐

- EC profile 已选定为 `6+4`（6 data + 4 parity，最多容忍 4 个 shard 丢失），必须继续统一所有文档、类型和测试。
- 明确 ObjectId 的域分离规则及版本迁移策略。
- 明确本地 root、集群 object descriptor、placement epoch 的权威关系。
- 定义“可丢失 shard”是独立 host、disk 还是 failure domain。
- 把这些选择写入 `DESIGN.md`、`FORMAT.md` 和 `INVARIANTS.md`，再开始下一批次。
