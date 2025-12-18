# CDC 水位恢复偏差问题复现指南

## 问题描述

在 MatrixOne 重启后，CDC 任务的水位恢复出现偏差：某些表的水位显示为"最新"，但上游和下游的数据行数不一致。

## 复现脚本

使用 `reproduce_cdc_watermark_bias.py` 脚本可以稳定复现这个问题。

## 前置条件

1. **安装 Python 依赖**：
   ```bash
   pip install pymysql
   ```

2. **确保 MatrixOne 正在运行**

3. **创建 CDC 任务**（脚本会提示，也可以提前创建）

## 使用方法

### 基本用法

```bash
# 使用默认配置（127.0.0.1:6001, user=dump, password=111）
python3 reproduce_cdc_watermark_bias.py
```

### 指定数据库连接

```bash
python3 reproduce_cdc_watermark_bias.py \
    --host 127.0.0.1 \
    --port 6001 \
    --user dump \
    --password 111
```

### 调整循环间隔

```bash
# 更快的循环（0.05 秒间隔，更容易触发问题）
python3 reproduce_cdc_watermark_bias.py --interval 0.05

# 更慢的循环（0.5 秒间隔）
python3 reproduce_cdc_watermark_bias.py --interval 0.5
```

### 启用后台监控

```bash
# 每 10 秒自动检查一次数据一致性
python3 reproduce_cdc_watermark_bias.py --monitor --monitor-interval 10
```

### 跳过初始化步骤

```bash
# 如果数据库和表已存在，跳过创建步骤
python3 reproduce_cdc_watermark_bias.py --skip-setup

# 跳过 CDC 任务检查
python3 reproduce_cdc_watermark_bias.py --skip-cdc-check
```

## 复现步骤

### 1. 启动脚本

```bash
python3 reproduce_cdc_watermark_bias.py --interval 0.1 --monitor
```

脚本会：
- 自动创建上游数据库 `cdc_src_repro` 和下游数据库 `cdc_sink_repro`
- 创建表 `t_cdc_repro`
- 检查 CDC 任务是否存在
- 开始运行插入+删除循环

### 2. 创建 CDC 任务（如果还没有）

如果脚本提示未找到 CDC 任务，需要手动创建：

```sql
-- 根据你的 CDC 语法创建任务
-- 示例（根据实际语法调整）：
CREATE CHANGEFEED cdc_repro_task
    INTO 'mysql://user:password@127.0.0.1:6001/cdc_sink_repro'
    FOR DATABASE cdc_src_repro;
```

### 3. 观察脚本运行

脚本会持续运行，每 100 次循环打印一次进度：

```
[17:20:48] 循环: 100, 插入: 100, 删除: 100, 速率: 10.0 次/秒
[17:20:58] 循环: 200, 插入: 200, 删除: 200, 速率: 10.0 次/秒
...
```

每 1000 次循环会自动检查一次数据一致性。

### 4. 重启 MatrixOne（关键步骤）

**在脚本运行过程中，突然重启 MatrixOne**：

```bash
# 方式1: 如果使用 systemd
sudo systemctl restart matrixone

# 方式2: 如果直接运行进程
kill -9 <mo-service-pid>
# 然后重新启动 mo-service

# 方式3: 如果使用 Docker
docker restart <container-name>
```

**关键时机**：
- 在脚本运行了至少几秒钟后（让 CDC 有足够的数据变更）
- 在脚本打印进度后立即重启（更容易触发问题）
- 可以多次尝试，因为问题出现的概率与重启时机有关

### 5. 观察问题

重启后，脚本会：
- 自动重连数据库
- 继续运行
- 在下次检查时（每 1000 次循环）发现数据不一致

如果启用了 `--monitor`，后台监控也会检测到不一致。

### 6. 查看日志

重启后，检查 MatrixOne 日志中的 CDC 相关日志：

```bash
# 查找水位恢复日志
grep "cdc.watermark.recovery" cdc.log

# 查找水位持久化日志
grep "cdc.watermark.persist" cdc.log

# 查找事务提交日志
grep "cdc.txn_manager.commit" cdc.log
```

重点关注：
- 重启前最后的水位持久化时间
- 重启后恢复的水位值
- 数据同步过程中的行数统计

### 7. 停止脚本

按 `Ctrl+C` 停止脚本，脚本会：
- 打印最终统计信息
- 执行最终的数据一致性检查
- 显示详细的数据差异（如果有）

## 预期结果

### 正常情况（无问题）

```
【最终检查】数据一致性
上游行数: 0
下游行数: 0
✅ 数据一致
```

### 问题复现（数据不一致）

```
【最终检查】数据一致性
上游行数: 0
下游行数: 1
❌ 数据不一致！差异: 1 行
仅在下游存在的数据: [(1, 1234)]
```

或者：

```
⚠️  [监控] 检测到数据不一致！
  上游: 0 行, 下游: 1 行
  时间: 2025-12-15 17:20:51
```

## 问题分析

当问题复现时，通常会出现以下情况：

1. **水位恢复偏差**：
   - 重启前最后提交的水位：`1765790448449538306-1`
   - 重启后恢复的水位：`1765790198618355489-1`（更旧）
   - 偏差：约 2.5 亿个时间戳单位

2. **数据不一致**：
   - 上游表为空（所有插入都被删除了）
   - 下游表有残留数据（某些删除操作没有正确同步）

3. **根本原因**：
   - 数据已写入下游，但水位还在内存中（`cacheUncommitted`）
   - 系统在 3 秒持久化窗口内崩溃
   - 重启后从数据库恢复的是旧水位，导致数据重复同步或丢失

## 调试建议

1. **增加日志级别**：确保 CDC 相关日志都开启
2. **多次尝试**：问题出现的概率与重启时机有关，可能需要多次尝试
3. **调整循环间隔**：更快的循环（`--interval 0.05`）更容易触发问题
4. **监控水位持久化**：观察 `cdc.watermark.persist.success` 日志，了解持久化频率
5. **对比日志时间**：对比重启前最后的水位持久化时间和重启时间

## 解决方案

参考 `CDC_WATERMARK_RECOVERY_BIAS_ANALYSIS.md` 中的解决方案建议。

