# local-workflow-orchestrator-go

一个纯本地、零外部依赖（仅 Go 标准库）的**工作流编排引擎**。它将带依赖的
任务图（DAG）在并发执行、失败重试、进程重启与运行期动态扩展下的调度结果做到
**确定且可复现**。

模块路径：`github.com/h3yow/local-workflow-orchestrator-go`（Go 1.25+）。

---

## 1. 包结构

| 包        | 职责                                                                 |
|-----------|----------------------------------------------------------------------|
| `spec`    | 声明式任务图规格与静态校验（环、悬空依赖、重复 ID、非法数值）          |
| `state`   | 运行/节点状态模型、尝试记录、深拷贝快照                                |
| `backoff` | 确定性退避策略（固定 / 线性 / 指数，无抖动，可选封顶）                 |
| `store`   | 目录式持久化：原子快照 + 仅追加、按键幂等去重的事件/效果日志           |
| `engine`  | 并发调度、重试、超时、跳过传播、背压、恢复、动态扩展与一致性视图        |
| `cmd/orchestrator` | 一个可运行的端到端演示                                     |

---

## 2. 规格语义（`spec.Spec`）

```go
s := &spec.Spec{
    ID: "my-graph",
    Nodes: []spec.NodeSpec{
        {ID: "a"},                                   // Slots 缺省归一化为 1
        {ID: "b", Deps: []string{"a"},
         Timeout: 2*time.Second, MaxRetries: 3, Slots: 2},
    },
}
s.Normalize()       // Slots==0 -> 1（其余字段不做隐式修正）
err := s.Validate() // 返回 *spec.RejectError，原因可机器区分
```

节点字段：

- `ID`：图内唯一、非空。
- `Deps`：依赖节点 ID。所有依赖都进入终态后节点才可调度。
- `Timeout`：**单次尝试**的上限；`0` 表示不限制；禁止负值。
- `MaxRetries`：首次尝试之后的重试**次数**（`0`=失败一次即终态失败）；禁止负值。
- `Slots`：节点运行时占用的并发容量（并发要求），必须 `>=1`，`0` 经
  `Normalize()` 归一为 `1`。

### 拒绝原因（互不混淆）

`Validate()` 返回 `*spec.RejectError{Reason, Subject, Message}`，`Reason` 为：

| Reason                       | 触发条件                                   |
|------------------------------|--------------------------------------------|
| `empty_id`                   | 规格 ID 或节点 ID 为空                     |
| `duplicate_id`               | 同一规格内节点 ID 重复                     |
| `dangling_dependency`        | 依赖指向不存在的节点                       |
| `cycle`                      | 依赖图存在环（含自依赖），错误信息含环路   |
| `invalid_value`              | 负超时/负重试/非法 Slots/重复依赖边/空图等 |

校验按**声明顺序**进行，因此同一份规格首次报告的缺陷原因是稳定的。

### 确定性拓扑序

`Topological()` 输出稳定拓扑序：在所有依赖已输出的节点中，按声明顺序输出
（Kahn 算法 + 声明顺序的就绪队列）。相同规格反复调用结果一致。

---

## 3. 调度规则

### 3.1 单线程事件循环 + 有界工作器

每个运行（run）由**唯一的事件循环 goroutine** 串行驱动状态变更；用户任务代码
运行在按槽位受限启动的 worker goroutine 中，worker 只能通过事件回报结果。
因此：

- 状态写入天然无竞争；观察者通过 `View()` 拿到的是**深拷贝快照**，绝不会读到
  半更新结构。
- 就绪节点只会被一个循环分发，**不会被重复执行，也不会永久遗漏**。

### 3.2 并发上限（`Config.MaxRunning`）

`MaxRunning` 限制**同一时刻所有运行中节点的 Slots 之和**。例如上限为 2 时，
一个 `Slots:2` 的节点单独占满容量；两个 `Slots:1` 的节点可并行。容量不足时，
就绪节点在队列中等待。

### 3.3 就绪顺序（可复现）

就绪队列排序键为 `(NextAttemptAt, OrderKey, ID)`：

1. 退避到期时间；
2. `OrderKey`——初始节点为声明序号，动态扩展节点为单调递增的全局序号；
3. ID 字典序兜底。

退避策略**不含随机抖动**，相同（策略, 尝试序号）永远产生相同延迟。因此相同
规格 + 相同任务行为下，调度顺序、尝试次数、最终状态与效果集合稳定可复现
（见 `TestReproducibleScheduling`）。

### 3.4 状态机

```
pending ──准入──> queued ──获得槽位──> running
                     ^                   │
                     │   失败/超时且仍有预算 │
                     └──────(退避)────────┘
                                         │ 成功
                                         v
                                     succeeded
running 失败/超时且预算耗尽 ──────────> failed
任一依赖 failed/skipped 的节点 ───────> skipped
```

- `skipped`（跳过）与 `failed`（失败）、未执行（`pending`）**明确可区分**。
- 失败节点的下游**永远不会被判定为成功**：直接或传递依赖到 failed/skipped
  的节点会被分类为 `skipped`；独立分支照常执行。
- 运行级状态：全部终态后，有 failed 则 `failed`，否则有 skipped 则 `skipped`，
  否则 `succeeded`。

### 3.5 重试与超时

- 一次尝试得到 `error` 或 `timeout` 才计入“失败次数”；成功结束重试。
- 当失败次数 `<= MaxRetries` 时，节点回到 `queued` 并按退避策略等待
  （fixed/linear/exponential，`Max` 封顶）。
- 失败次数 `> MaxRetries` → 节点 `failed`，原因字符串明确写出
  “retries exhausted …”。
- 单尝试 `context.WithTimeout` 超时 → 该尝试结果记为 `timeout`；无剩余预算时
  节点终态 `failed`。每次尝试都在 `NodeState.Attempts` 留有不可变记录
  （`success/error/timeout/interrupted` + 原因 + 起止时间）。

---

## 4. 背压策略（`Config.MaxQueue`）

引擎维护一个**有界就绪/退避队列**，其占用规模为“已准入但未运行”的节点数 `P`：

- 符合准入条件的 `pending` 节点仅在 `P < MaxQueue` 时进入队列；否则保持
  `pending`，**不会报错丢弃，也不会无界占用内存**。
- 每次有节点离开队列（开始运行）、终态或退避到期后，调度器都会重新扫描
  `pending` 集合，因此被暂时拒绝的有效任务在腾出空间后**必然被重新准入**，
  不会静默丢失，也不会被重复执行。
- **动态扩展同样受背压约束且为原子操作**：一次扩展中“立即可调度”的新节点若
  不能全部放进剩余队列容量，则整批扩展返回 `engine.ErrBackpressure`，
  **没有任何部分生效**。

---

## 5. 持久化与恢复

### 5.1 磁盘布局

```
<storeDir>/<runID>/snapshot.json   # 最新全量运行状态（临时文件 + rename 原子写）
<storeDir>/<runID>/events.log      # 仅追加 JSON-lines 事件/效果历史
```

- 快照在每个调度决策批次后原子落盘；读者只会看到上一版或新版完整文件，
  不会看到半个文件。
- 事件（含副作用 effect）以 `Key` 幂等追加：重复 Key 是空操作。

### 5.2 崩溃恢复（`Engine.Resume`）

进程在任意调度点被 kill 后，用同一存储目录新建引擎并 `Resume(runID, ...)`：

1. **已 `succeeded` 的节点不会重跑**（以落盘状态为准）。
2. 崩溃时处于 `running` 的节点，其最后一次尝试被追加记录为
   `interrupted`（**绝不静默标记成功**），节点重新排队再执行。
   “中断”不消耗重试预算（崩溃既非任务失败也非超时）。
3. `queued` 且处于退避窗口内的节点，按剩余等待时间重新挂定时器。
4. 事件日志被重放以重建去重索引；即使“事件已追加、快照尚未更新”窗口内崩溃，
   **副作用记录也不会重复**。effect 去重锚点为 `effect:<node>:<effectKey>`。
5. 历史运行通过 `ListRuns()` 可枚举，彼此隔离，一个运行的失败不污染其他运行。

### 5.3 提交窗口与至少一次（at-least-once）语义

每个调度决策批次结束后立即以“临时文件 + rename”原子写入快照。恢复的事实
依据永远是**最后一份已持久化的快照**：

- 已经以 `succeeded` 落盘的节点**绝不会重跑**。
- 若进程恰好死在“某次状态刚变迁、对应快照尚未落盘”的极短窗口内，恢复会
  基于上一份快照把该尝试当作 `interrupted` **重放一次**（at-least-once）。
- 因此用户任务应能用 **effect key** 表达其外部副作用：恢复/重试下相同
  `effect:<node>:<key>` 只记录一次，事件日志去重 + 快照去重双保险，
  **不会产生重复的副作用记录**。这同时也是“执行中节点不被静默标记成功”
  与“副作用恰好一次可见”得以并存的关键。

---

## 6. 运行期动态扩展

两种方式：

- 声明 `Option.Expander`：节点成功后回调返回一批 `NodeSpec`（hook 扩展）。
- 运行期调用 `Engine.Expand(runID, parentID, nodes)`（显式扩展，**允许对已终态
  运行追加**，追加有效节点后运行重新变为可调度）。

扩展语义：

- 合并后的整图重新做完整校验：悬空依赖、环、非法值分别返回可区分错误
  （`errDanglingDependency` / `errExpansionCycle` / `errInvalidExpansion`）。
- 新节点 ID 若在运行中已存在、或在同一批中重复，返回 `errDuplicateNode`；
  **同一节点无论被生产多少次，只生效一次**。
- 只有 `succeeded` 的父节点允许扩展。
- 依赖了 failed/skipped 上游的新节点立即分类为 `skipped`。
- 新节点 `OrderKey` 单调递增，准入受 `MaxQueue` 背压约束（原子，见 §4）。

---

## 7. 多实例 / 观察者一致性

- 每个运行单写者（事件循环），观察者通过 `View()` 获得加锁下产生的深拷贝；
  状态集合大小、各节点状态始终自洽，成功集合单调不回退。
- 引擎可同时驱动多个运行，运行间状态、effect、历史完全隔离。
- `Wait()` 在每次“运行进入终态”时返回；之后若显式扩展使其复活，可再次
  `Wait()` 等待新的终态。

---

## 8. 验证方法

运行全部测试（推荐带竞态检测）：

```bash
go test ./... -race -count=1
```

运行演示：

```bash
go run ./cmd/orchestrator
```

### 测试覆盖重点（日志会打印输入规格与状态流转依据）

| 测试                                  | 验证内容                                                   |
|---------------------------------------|------------------------------------------------------------|
| `spec` 包全部用例                      | 环/自环、悬空依赖、重复 ID、空 ID、非法值、确定性拓扑        |
| `TestFailureAndSkipClassification`    | 依赖失败、失败与跳过正确归类、独立分支不受影响、原因可区分   |
| `TestRetryExhausted` / `TestRetryThenSucceed` | 重试次数 = MaxRetries+1、退避后成功、效果记录      |
| `TestTimeoutFailsNode`                | 超时尝试分类为 `timeout`、耗尽后节点 `failed`               |
| `TestConcurrencyCap`                  | 运行中 Slots 之和绝不超过上限                               |
| `TestBackpressure`                    | 有界队列不丢任务、全部最终执行、日志含背压依据               |
| `TestRecoveryMidExecution`            | 执行中“杀掉”引擎后用新引擎 Resume：不重跑成功节点、运行中节点被记中断再执行、效果不重复 |
| `TestRecoveryFromCraftedRunningSnapshot` | 手工构造崩溃瞬间磁盘快照，精确验证恢复语义                |
| `TestDynamicExpansion` 及 `TestExpansion*` | 动态扩展、重复生产只生效一次、环/悬空/背压拒绝原因可区分  |
| `TestConcurrentViewsConsistent`       | 并发观察者只见一致快照、成功集合单调                        |
| `TestMultiRunIsolation`               | 多运行并发互不污染、历史保留、各运行 effect 隔离             |
| `TestReproducibleScheduling`          | 相同规格两次运行的状态/尝试次数/效果集合完全一致            |
| `TestNoDuplicateExecutionUnderContention` | 单槽位 + 高并发观察者下每个节点恰好执行一次              |

日志通过 `Config.Logger` 注入；测试使用带互斥保护的内存缓冲 logger，在失败时
通过 `t.Log` 打印完整的“准入 → 运行 → 成功/重试/失败 → 跳过 → 运行终态”
流转依据。

---

## 9. 快速上手

```go
st, _ := store.NewFileStore("./data")
eng, _ := engine.New(engine.Config{
    MaxRunning: 4, MaxQueue: 64, Store: st,
    Backoff: backoff.Policy{Kind: backoff.Exponential, Base: 10*time.Millisecond, Max: time.Second},
})
defer eng.Close()

runID, err := eng.Create(ctx, mySpec,
    func(ctx context.Context, runID, nodeID string) (string, []string, error) {
        // 业务执行；返回的 effectKeys 在重试/恢复下全局只记录一次
        return "result", []string{"side-effect-key"}, nil
    },
    engine.Option{RunID: "run-1"})
if err != nil { /* *spec.RejectError，可按 Reason 区分 */ }

snap, err := eng.Wait(ctx, runID) // *state.RunState
```
