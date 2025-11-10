# CDC GC 功能测试脚本使用说明

## 概述

`test_cdc_gc.sh` 是一个全面的CDC（Change Data Capture）GC功能测试脚本，用于验证CDC功能在GC过程中的数据保护机制。

## 测试场景

脚本包含以下7个测试场景：

### 场景1: 基本CDC保护测试
- 测试基本的CDC数据保护功能
- 验证设置了watermark后，数据不会被GC删除

### 场景2: 最小watermark策略
- 测试同一数据库多个表的watermark取最小值策略
- 验证数据库级别的保护机制

### 场景3: 多数据库CDC保护
- 测试多个数据库的独立CDC保护
- 验证只有设置了CDC的数据库被保护

### 场景4: 数据边界测试（应该被GC的数据）
- 测试watermark边界条件
- 验证旧数据可以被GC，新数据被保护

### 场景5: 空watermark测试
- 测试空watermark表示保护所有数据的行为
- 验证边界情况处理

### 场景6: watermark更新测试
- 测试watermark更新后的保护行为
- 验证动态更新机制

### 场景7: 并发多表CDC保护
- 测试多个表同时设置CDC保护
- 验证并发场景下的正确性

## 使用方法

### 基本用法

```bash
./test_cdc_gc.sh
```

### 自定义连接参数

```bash
# 设置数据库连接参数
export DB_HOST=127.0.0.1
export DB_PORT=6001
export DB_USER=dump
export DB_PASS=111
export ACCOUNT_ID=0

./test_cdc_gc.sh
```

### 环境变量说明

- `DB_HOST`: 数据库主机地址（默认: 127.0.0.1）
- `DB_PORT`: 数据库端口（默认: 6001）
- `DB_USER`: 数据库用户名（默认: dump）
- `DB_PASS`: 数据库密码（默认: 111）
- `ACCOUNT_ID`: 账户ID（默认: 0）

## 前置条件

1. **数据库连接**: 确保可以连接到MatrixOne数据库
2. **CDC功能**: 确保CDC功能已启用，`mo_cdc_watermark`表存在
3. **GC配置**: 确保GC功能已启用
4. **时间配置**: 建议调整GC、checkpoint和flush的时间间隔以便测试：
   - 缩短checkpoint间隔以便快速看到效果
   - 缩短GC间隔以便快速验证
   - 缩短flush间隔以便数据快速落盘

## 配置建议

为了确保测试能够快速验证结果，建议在测试前调整以下配置：

### 1. 缩短Checkpoint间隔

```sql
-- 查看当前checkpoint配置
SHOW VARIABLES LIKE '%checkpoint%';

-- 调整checkpoint间隔（示例，具体参数名可能不同）
SET GLOBAL checkpoint_interval = '30s';
```

### 2. 缩短GC间隔

```sql
-- 查看当前GC配置
SHOW VARIABLES LIKE '%gc%';

-- 调整GC间隔（示例，具体参数名可能不同）
SET GLOBAL gc_interval = '60s';
```

### 3. 缩短Flush间隔

```sql
-- 查看当前flush配置
SHOW VARIABLES LIKE '%flush%';

-- 调整flush间隔（示例，具体参数名可能不同）
SET GLOBAL flush_interval = '10s';
```

## 测试流程

1. **准备阶段**: 脚本会自动检查数据库连接和必要表
2. **执行测试**: 依次运行7个测试场景
3. **验证结果**: 每个场景都会验证数据是否正确保护/删除
4. **清理**: 每个场景结束后自动清理测试数据

## 预期结果

### 成功情况
- 所有测试场景通过
- 设置了CDC保护的数据未被GC删除
- 应该被GC的数据被正确删除
- 最小watermark策略正确工作

### 失败情况
- 如果测试失败，脚本会输出错误信息
- 检查日志了解具体失败原因
- 验证GC和checkpoint配置是否正确

## 故障排查

### 问题1: 连接失败
```
[ERROR] 无法连接到数据库，请检查连接参数
```
**解决方案**: 检查DB_HOST、DB_PORT、DB_USER、DB_PASS环境变量

### 问题2: 表不存在
```
[ERROR] mo_cdc_watermark表不存在
```
**解决方案**: 确保CDC功能已启用，表已创建

### 问题3: 数据被错误删除
```
[ERROR] 测试失败: 数据被错误删除
```
**解决方案**: 
- 检查GC配置是否正确
- 检查watermark时间戳是否正确
- 检查checkpoint是否正常执行

### 问题4: 测试超时
**解决方案**: 
- 增加wait_for_checkpoint和wait_for_gc的等待时间
- 检查系统负载是否过高
- 调整GC和checkpoint间隔

## 注意事项

1. **时间同步**: 确保系统时间准确，watermark时间戳依赖系统时间
2. **数据隔离**: 测试脚本会创建和删除测试数据库，不会影响生产数据
3. **并发测试**: 如果同时运行多个测试实例，请使用不同的数据库名称
4. **资源占用**: 测试会创建多个数据库和表，注意资源使用情况

## 扩展测试

如果需要添加更多测试场景，可以：

1. 在脚本中添加新的测试函数
2. 在main函数中调用新测试函数
3. 遵循现有的测试模式：
   - 创建测试数据
   - 设置CDC watermark
   - 等待checkpoint和GC
   - 验证结果
   - 清理数据

## 示例输出

```
[INFO] 开始CDC GC功能测试...
[INFO] 数据库连接: dump@127.0.0.1:6001
[INFO] 账户ID: 0

[INFO] ========== 测试场景1: 基本CDC保护 ==========
[INFO] 插入测试数据...
[INFO] 插入CDC watermark: 2024-01-01 12:00:00.000000
[INFO] 等待checkpoint完成（最多30秒）...
[INFO] Checkpoint已完成
[INFO] 表 test_table1 中的数据行数: 5
[INFO] ✓ 测试通过: 数据被正确保护
[INFO] 场景1测试完成

...
```

## 联系支持

如果遇到问题，请：
1. 检查日志输出
2. 验证配置是否正确
3. 查看MatrixOne文档
4. 联系开发团队

