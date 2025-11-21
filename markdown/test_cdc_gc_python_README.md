# CDC GC 测试脚本 (Python版本) 使用说明

## 概述

`test_cdc_gc.py` 是一个用Python编写的CDC GC功能测试脚本，支持：
- **并发操作**：多线程并发插入数据到多个表
- **长时间运行**：支持长时间测试（默认30分钟，可配置）
- **智能等待**：watermark更新等待机制，带超时和重试
- **详细日志**：完整的日志记录，便于排查问题
- **错误处理**：完善的错误处理和异常捕获

## 安装依赖

```bash
pip install -r test_cdc_gc_requirements.txt
```

或者直接安装：

```bash
pip install pymysql
```

## 使用方法

### 基本用法

```bash
python3 test_cdc_gc.py
```

### 自定义连接参数

```bash
export DB_HOST=127.0.0.1
export DB_PORT=6001
export DB_USER=dump
export DB_PASS=111
export ACCOUNT_ID=0
python3 test_cdc_gc.py
```

### 自定义测试时长

```bash
# 运行60分钟
export TEST_DURATION=60
python3 test_cdc_gc.py

# 运行120分钟（2小时）
export TEST_DURATION=120
python3 test_cdc_gc.py
```

## 环境变量

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| DB_HOST | 127.0.0.1 | 数据库主机地址 |
| DB_PORT | 6001 | 数据库端口 |
| DB_USER | dump | 数据库用户名 |
| DB_PASS | 111 | 数据库密码 |
| ACCOUNT_ID | 0 | 账户ID |
| TEST_DURATION | 30 | 长时间测试运行时长（分钟） |

## 测试场景

### 场景1: 基本CDC保护测试
- 创建CDC任务
- 插入测试数据
- 等待watermark更新（最多120秒）
- 查询并验证watermark

### 场景2: 并发多表操作
- 创建多个表（5个表）
- 并发插入数据到多个表
- 每个表使用3个线程并发插入
- 等待watermark更新
- 验证所有表的watermark

### 场景3: 长时间运行测试
- 创建CDC任务
- 持续并发插入数据（默认30分钟）
- 每5轮查询一次watermark
- 每分钟执行一轮数据插入
- 适合长时间稳定性测试

## 特性说明

### 1. 并发操作
- 使用 `ThreadPoolExecutor` 实现多线程并发
- 每个表可以独立并发插入数据
- 支持自定义线程数和每线程插入数量

### 2. 智能等待
- `wait_for_watermark()` 函数带超时机制
- 默认超时120秒，可配置
- 每10秒检查一次watermark
- 每30秒打印一次等待状态

### 3. 长时间运行
- 支持长时间运行测试（默认30分钟）
- 可通过环境变量 `TEST_DURATION` 配置
- 持续监控watermark更新
- 适合验证CDC任务的稳定性

### 4. 错误处理
- 完善的异常捕获和错误日志
- SQL执行失败会记录详细错误信息
- 连接失败会自动重试
- 所有异常都会被记录

## 日志说明

脚本会输出详细的日志信息：

```
2024-01-01 12:00:00 [INFO] 开始CDC GC功能测试...
2024-01-01 12:00:00 [INFO] 数据库连接: dump@127.0.0.1:6001
2024-01-01 12:00:00 [INFO] 账户ID: 0
2024-01-01 12:00:00 [INFO] 
2024-01-01 12:00:00 [INFO] 开始运行测试场景...
2024-01-01 12:00:00 [INFO] ============================================================
2024-01-01 12:00:00 [INFO] 测试场景1: 基本CDC保护
2024-01-01 12:00:00 [INFO] ============================================================
...
```

## 注意事项

1. **watermark更新延迟**：
   - watermark更新不是实时的，需要等待
   - 脚本会自动等待watermark更新（最多120秒）
   - 如果超时，会记录警告但不会失败

2. **长时间运行**：
   - 长时间测试会持续插入数据
   - 建议在测试环境中运行
   - 可以通过日志监控CDC任务状态

3. **并发操作**：
   - 并发操作会增加数据库负载
   - 建议根据实际情况调整线程数
   - 监控数据库性能指标

4. **任务管理**：
   - 测试结束后，CDC任务不会被删除
   - 可以继续使用或手动清理
   - 查看CDC任务日志确认是否有错误

## 查看CDC任务日志

测试过程中，建议同时查看CDC任务日志：

```bash
# 查看CDC任务状态
SHOW CDC TASKS;

# 查看特定任务的watermark
SELECT db_name, table_name, watermark, err_msg 
FROM mo_catalog.mo_cdc_watermark 
WHERE account_id=0 AND task_id IN (
    SELECT task_id FROM mo_catalog.mo_cdc_task WHERE task_name='test_cdc_task1'
);
```

## 故障排查

### 问题1: 连接失败
```
[ERROR] 数据库连接失败: ...
```

**解决方案**:
- 检查DB_HOST、DB_PORT是否正确
- 检查数据库服务是否运行
- 检查防火墙设置

### 问题2: watermark更新超时
```
[WARNING] ⚠ watermark更新超时（120秒）
```

**解决方案**:
- 这是正常的，watermark更新需要时间
- 可以增加超时时间
- 检查CDC任务是否正常运行
- 查看CDC任务日志是否有错误

### 问题3: 并发插入失败
```
[ERROR] 线程 X 插入失败: ...
```

**解决方案**:
- 检查表结构是否正确
- 检查数据库连接是否稳定
- 减少并发线程数
- 增加插入间隔时间

## 与Shell版本的区别

| 特性 | Shell版本 | Python版本 |
|------|-----------|------------|
| 并发操作 | ❌ | ✅ |
| 长时间运行 | ❌ | ✅ |
| 智能等待 | ❌ | ✅ |
| 错误处理 | 基础 | 完善 |
| 日志记录 | 基础 | 详细 |
| 代码维护 | 中等 | 容易 |

## 扩展测试

如果需要添加更多测试场景，可以：

1. 在 `CDCTester` 类中添加新方法
2. 创建新的测试函数
3. 在 `main()` 函数中调用新测试函数

示例：

```python
def test_custom_scenario(tester: CDCTester):
    """自定义测试场景"""
    logger.info("自定义测试场景")
    # 你的测试代码
    return True

# 在main()中调用
test_custom_scenario(tester)
```

## 联系支持

如果遇到问题，请：
1. 查看日志输出
2. 检查CDC任务日志
3. 验证配置是否正确
4. 联系开发团队

