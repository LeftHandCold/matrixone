# GC文件删除失败后的恢复机制分析

## 问题
如果先更新watermark，后删除文件，删除失败的话，下次GC是否可以继续删除这些文件？

## GC执行流程分析

### 正常GC流程

1. **`doGCAgainstGlobalCheckpointLocked`执行**（`line 1225-1242`）：
   ```go
   filesToGC, metafile, err = scannedWindow.ExecuteGlobalCheckpointBasedGC(...)
   ```
   - 基于当前的`scannedWindow.files`（包含之前扫描的checkpoint中的文件列表）
   - 基于当前的global checkpoint (`gckp`)
   - 基于snapshots、pitrs、cdcWatermarks等
   - 确定哪些文件可以GC（不在snapshots、pitrs等中的文件）
   - **关键**：更新`w.files = filesNotGC`（`window.go:171`），只保留不能GC的文件
   - 返回`filesToGC`（可以GC的文件列表）

2. **写入WAL**（`line 1244-1252`）：
   - 将新的metadata文件路径写入WAL
   - 如果失败，返回错误，但`scannedWindow.files`已经更新

3. **更新metadata**（`line 1254-1259`）：
   - 更新`c.mutation.metaFiles`

4. **更新watermark**（`line 1270`）：
   ```go
   c.updateGCWaterMark(gckp)  // watermark更新到gckp的end
   ```

5. **删除文件**（`line 1142-1148`）：
   ```go
   if err = c.deleter.DeleteMany(ctx, c.TaskNameLocked(), filesToGC); err != nil {
       return  // 如果这里失败，但watermark已经更新
   }
   ```

### 删除失败后的状态

**如果文件删除失败**：
- ✅ `scannedWindow.files`已经更新为`filesNotGC`（不包含已GC的文件）
- ✅ watermark已经更新到`gckp.GetEnd()`
- ✅ WAL已经写入新的metadata文件路径
- ❌ 文件实际还在磁盘上（删除失败）

## 下次GC时的行为分析

### 场景1：下次GC正常执行

**下次GC调用时**（`tryGCLocked`）：
1. 获取新的maxGlobalCKP（`line 1032`）
2. 检查watermark（`line 1045-1051`）：
   ```go
   gcWaterMarkTS := gcWaterMark.GetEnd()
   maxGlobalCKPTS := maxGlobalCKP.GetEnd()
   if gcWaterMarkTS.GE(&maxGlobalCKPTS) {
       return  // 如果watermark >= maxGlobalCKP，不执行GC
   }
   ```
   - 如果新的maxGlobalCKP的end <= 上次的watermark，GC不会执行
   - 只有当新的maxGlobalCKP的end > 上次的watermark时，才会执行GC

3. 如果执行GC，调用`tryGCAgainstGCKPLocked`：
   - 获取当前的`scannedWindow`（`line 1220`）
   - `scannedWindow.files`已经是`filesNotGC`，不包含上次删除失败的文件
   - 基于新的global checkpoint和snapshots执行GC
   - **关键问题**：上次删除失败的文件不在`scannedWindow.files`中，也不会在新的checkpoint中

### 场景2：GC状态恢复（Replay）

**系统重启后，GC状态从metadata文件恢复**（`Replay`函数）：
1. 读取GC目录中的metadata文件（`line 331-335`）
2. 恢复`scannedWindow`（`line 385-404`）：
   ```go
   window.ReadTable(ctx, ioutil.MakeGCFullName(name), c.fs)
   c.mutAddScannedLocked(window)
   ```
   - 从metadata文件读取`filesNotGC`（不能GC的文件）
   - 恢复`scannedWindow.files`
   - **关键**：metadata文件中只包含`filesNotGC`，不包含已GC的文件

3. 恢复watermark（`line 418, 423`）

**结论**：即使从metadata文件恢复，`scannedWindow.files`也只包含`filesNotGC`，不包含上次删除失败的文件。

## 关键发现：文件泄漏风险

### 问题分析

**删除失败的文件无法被下次GC识别**，原因：

1. **`scannedWindow.files`已经更新**：
   - `ExecuteGlobalCheckpointBasedGC`执行后，`w.files = filesNotGC`（`window.go:171`）
   - 只保留不能GC的文件，已GC的文件已经从列表中移除

2. **Watermark已经更新**：
   - `gcWaterMark`已经更新到`gckp.GetEnd()`
   - 下次GC会基于新的watermark继续，不会重新处理旧的checkpoint

3. **文件不在新的checkpoint中**：
   - 删除失败的文件属于旧的checkpoint范围
   - 新的checkpoint不会包含这些文件（因为它们已经被标记为GC）

4. **Metadata文件只包含`filesNotGC`**：
   - `writeMetaForRemainings`只写入不能GC的文件（`window.go:166-170`）
   - 即使系统重启，从metadata恢复时，也不会包含删除失败的文件

### 文件泄漏场景

**场景1：部分文件删除失败**
- 假设`filesToGC = [f1, f2, f3, f4, f5]`
- 删除时，f1, f2, f3成功，f4, f5失败
- `scannedWindow.files`已经更新，不包含f1-f5
- watermark已经更新
- **结果**：f4, f5永远无法被再次识别为需要删除

**场景2：全部文件删除失败**
- 假设`filesToGC = [f1, f2, f3]`
- 删除全部失败（磁盘满、权限问题等）
- `scannedWindow.files`已经更新，不包含f1-f3
- watermark已经更新
- **结果**：f1, f2, f3永远无法被再次识别为需要删除

**场景3：系统崩溃在删除过程中**
- watermark已经更新
- 部分文件已删除，部分未删除
- 系统重启后，从metadata恢复
- **结果**：未删除的文件无法被再次识别

## 可能的缓解机制

### 1. 检查：是否有其他机制可以识别这些文件？

**检查点1：新的checkpoint是否包含这些文件？**
- ❌ 不会。这些文件属于旧的checkpoint范围，新的checkpoint不会包含它们

**检查点2：是否有定期扫描机制？**
- ❌ 没有。GC只基于checkpoint和snapshot，不会扫描所有文件

**检查点3：是否有文件系统级别的清理？**
- ❌ 没有。GC是应用级别的，不会进行文件系统扫描

### 2. 检查：DeleteMany是否有重试机制？

查看`deleter.go`：
- `DeleteMany`按批次删除文件
- 如果某个批次失败，会返回错误
- 已删除的文件会从`g.toDeletePaths`中移除（`line 123`）
- **没有重试机制**，失败的文件不会自动重试

### 3. 检查：是否有其他清理机制？

查看代码，没有发现：
- 定期扫描未删除文件的机制
- 重试删除失败文件的机制
- 文件系统级别的清理机制

## 结论

### ⚠️ **严重问题：文件永久泄漏**

**如果文件删除失败，这些文件将无法被下次GC识别和删除**，原因：

1. **`scannedWindow.files`已经更新**：已GC的文件已经从列表中移除
2. **Watermark已经更新**：下次GC不会重新处理旧的checkpoint
3. **文件不在新的checkpoint中**：新的checkpoint不会包含这些文件
4. **Metadata文件只包含`filesNotGC`**：即使系统重启，也不会包含删除失败的文件

### 影响

- **磁盘空间泄漏**：删除失败的文件会永久占用磁盘空间
- **文件积累**：多次GC删除失败会导致文件持续积累
- **无法自动恢复**：没有机制可以自动识别和删除这些文件

### 建议的修复方案

1. **先删除文件，再更新watermark**：
   - 如果删除失败，不更新watermark
   - 下次GC会重新尝试删除

2. **记录删除失败的文件**：
   - 维护一个"待删除文件列表"
   - 下次GC时优先处理这些文件

3. **实现重试机制**：
   - 对删除失败的文件进行重试
   - 设置最大重试次数和重试间隔

4. **使用事务性操作**：
   - 确保watermark更新和文件删除的原子性
   - 如果删除失败，回滚watermark

5. **定期扫描和清理**：
   - 定期扫描文件系统，识别"孤立"文件
   - 基于watermark和checkpoint判断文件是否可以删除

## 代码位置

- GC执行：`pkg/vm/engine/tae/db/gc/v3/checkpoint.go:1131-1149`
- Watermark更新：`pkg/vm/engine/tae/db/gc/v3/checkpoint.go:1270`
- Window更新：`pkg/vm/engine/tae/db/gc/v3/window.go:171`
- 文件删除：`pkg/vm/engine/tae/db/gc/v3/deleter.go:60-132`
- 状态恢复：`pkg/vm/engine/tae/db/gc/v3/checkpoint.go:279-418`


