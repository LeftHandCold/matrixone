// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
)

const (
	// BloomFilter parameters
	bfEstimateRows = 100000  // Estimated number of objects
	bfProbability  = 0.001   // False positive rate 0.1%
)

// SyncProtectionRequest 同步保护请求
type SyncProtectionRequest struct {
	JobID   string `json:"job_id"`
	BF      string `json:"bf"`       // Base64 encoded BloomFilter
	ValidTS int64  `json:"valid_ts"`
}

// SyncProtectionTester 同步保护测试器
type SyncProtectionTester struct {
	db             *sql.DB
	dataDir        string
	jobID          string
	protectedFiles []string
	sampleCount    int
	verbose        bool
	waitTime       int
	mp             *mpool.MPool
}

func NewSyncProtectionTester(dsn, dataDir string, sampleCount int, verbose bool, waitTime int) (*SyncProtectionTester, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping 数据库失败: %w", err)
	}

	mp, err := mpool.NewMPool("sync_protection_test", 0, mpool.NoFixed)
	if err != nil {
		return nil, fmt.Errorf("创建内存池失败: %w", err)
	}

	return &SyncProtectionTester{
		db:          db,
		dataDir:     dataDir,
		jobID:       fmt.Sprintf("sync-test-%d", time.Now().UnixNano()),
		sampleCount: sampleCount,
		verbose:     verbose,
		waitTime:    waitTime,
		mp:          mp,
	}, nil
}

func (t *SyncProtectionTester) Close() {
	if t.db != nil {
		t.db.Close()
	}
}

// ScanObjectFiles 扫描目录获取 object 文件
func (t *SyncProtectionTester) ScanObjectFiles() ([]string, error) {
	var objects []string

	err := filepath.Walk(t.dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		// 匹配 object 文件名模式
		name := info.Name()
		// MatrixOne object 文件通常是 UUID 格式，包含下划线
		// 格式: 019c226d-9e98-7ecc-9662-712ff0edcbfb_00000 (42 字符)
		if len(name) == 42 && strings.Contains(name, "_") && strings.Count(name, "-") == 4 {
			objects = append(objects, name)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("扫描目录失败: %w", err)
	}

	return objects, nil
}

// SelectRandomObjects 随机选择 object
func (t *SyncProtectionTester) SelectRandomObjects(objects []string, count int) []string {
	if len(objects) <= count {
		return objects
	}

	// 复制切片避免修改原数据
	copied := make([]string, len(objects))
	copy(copied, objects)

	// 随机打乱
	rand.Shuffle(len(copied), func(i, j int) {
		copied[i], copied[j] = copied[j], copied[i]
	})

	return copied[:count]
}

// BuildBloomFilter 构建 BloomFilter
func (t *SyncProtectionTester) BuildBloomFilter(objects []string) (string, error) {
	fmt.Println("[DEBUG-BF] ========== 开始构建 BloomFilter ==========")
	fmt.Printf("[DEBUG-BF] 对象数量: %d\n", len(objects))
	fmt.Printf("[DEBUG-BF] 估计行数: %d, 误报率: %f\n", len(objects)+1000, bfProbability)
	
	// 打印所有要保护的对象
	fmt.Println("[DEBUG-BF] 要保护的对象列表:")
	for i, obj := range objects {
		fmt.Printf("[DEBUG-BF]   [%d] %s (len=%d, bytes=%v)\n", i, obj, len(obj), []byte(obj)[:min(20, len(obj))])
	}
	
	// Create BloomFilter
	bf := bloomfilter.New(int64(len(objects)+1000), bfProbability)
	defer bf.Free()

	// Create vector and add objects
	vec := vector.NewVec(types.T_varchar.ToType())
	defer vec.Free(t.mp)

	fmt.Println("[DEBUG-BF] 添加对象到 vector...")
	for i, obj := range objects {
		if err := vector.AppendBytes(vec, []byte(obj), false, t.mp); err != nil {
			return "", fmt.Errorf("添加对象到 vector 失败: %w", err)
		}
		if i < 3 {
			fmt.Printf("[DEBUG-BF]   添加: %s\n", obj)
		}
	}
	fmt.Printf("[DEBUG-BF] Vector 长度: %d\n", vec.Length())

	// Add to BloomFilter
	fmt.Println("[DEBUG-BF] 调用 bf.Add(vec)...")
	bf.Add(vec)

	// Verify BloomFilter works correctly before serialization
	fmt.Println("[DEBUG-BF] ========== 验证 BloomFilter (序列化前) ==========")
	testVec := vector.NewVec(types.T_varchar.ToType())
	defer testVec.Free(t.mp)
	
	preSerializeFailCount := 0
	for i, obj := range objects {
		testVec.Reset(types.T_varchar.ToType())
		if err := vector.AppendBytes(testVec, []byte(obj), false, t.mp); err != nil {
			fmt.Printf("[DEBUG-BF] ✗ 创建测试 vector 失败: %v\n", err)
			continue
		}
		result := bf.TestRow(testVec, 0)
		if result {
			if i < 5 || t.verbose {
				fmt.Printf("[DEBUG-BF] ✓ [%d] BloomFilter 包含: %s\n", i, obj)
			}
		} else {
			fmt.Printf("[DEBUG-BF] ✗ [%d] BloomFilter 不包含: %s (这是个问题!)\n", i, obj)
			preSerializeFailCount++
		}
	}
	if preSerializeFailCount > 0 {
		return "", fmt.Errorf("BloomFilter 序列化前验证失败: %d 个对象未找到", preSerializeFailCount)
	}
	fmt.Printf("[DEBUG-BF] ✓ 序列化前验证通过: 所有 %d 个对象都能找到\n", len(objects))

	// Marshal BloomFilter
	fmt.Println("[DEBUG-BF] ========== 序列化 BloomFilter ==========")
	data, err := bf.Marshal()
	if err != nil {
		return "", fmt.Errorf("序列化 BloomFilter 失败: %w", err)
	}
	
	// 计算原始数据的 hash
	rawHash := sha256.Sum256(data)
	rawHashStr := hex.EncodeToString(rawHash[:])
	fmt.Printf("[DEBUG-BF] 原始数据长度: %d bytes\n", len(data))
	fmt.Printf("[DEBUG-BF] 原始数据 SHA256: %s\n", rawHashStr)
	fmt.Printf("[DEBUG-BF] 原始数据前64字节: %v\n", data[:min(64, len(data))])

	// Verify BloomFilter works correctly after deserialization
	fmt.Println("[DEBUG-BF] ========== 验证 BloomFilter (反序列化后) ==========")
	bf2 := &bloomfilter.BloomFilter{}
	if err := bf2.Unmarshal(data); err != nil {
		return "", fmt.Errorf("反序列化 BloomFilter 失败: %w", err)
	}
	defer bf2.Free()
	
	testVec2 := vector.NewVec(types.T_varchar.ToType())
	defer testVec2.Free(t.mp)
	
	postDeserializeFailCount := 0
	for i, obj := range objects {
		testVec2.Reset(types.T_varchar.ToType())
		if err := vector.AppendBytes(testVec2, []byte(obj), false, t.mp); err != nil {
			fmt.Printf("[DEBUG-BF] ✗ 创建测试 vector 失败: %v\n", err)
			continue
		}
		result := bf2.TestRow(testVec2, 0)
		if result {
			if i < 5 || t.verbose {
				fmt.Printf("[DEBUG-BF] ✓ [%d] 反序列化后 BloomFilter 包含: %s\n", i, obj)
			}
		} else {
			fmt.Printf("[DEBUG-BF] ✗ [%d] 反序列化后 BloomFilter 不包含: %s (这是个问题!)\n", i, obj)
			postDeserializeFailCount++
		}
	}
	if postDeserializeFailCount > 0 {
		return "", fmt.Errorf("BloomFilter 反序列化后验证失败: %d 个对象未找到", postDeserializeFailCount)
	}
	fmt.Printf("[DEBUG-BF] ✓ 反序列化后验证通过: 所有 %d 个对象都能找到\n", len(objects))

	// Base64 encode
	fmt.Println("[DEBUG-BF] ========== Base64 编码 ==========")
	base64Data := base64.StdEncoding.EncodeToString(data)
	fmt.Printf("[DEBUG-BF] Base64 编码后长度: %d\n", len(base64Data))
	fmt.Printf("[DEBUG-BF] Base64 前100字符: %s\n", base64Data[:min(100, len(base64Data))])
	
	// 验证 Base64 解码后数据一致性
	fmt.Println("[DEBUG-BF] ========== 验证 Base64 解码一致性 ==========")
	decodedData, err := base64.StdEncoding.DecodeString(base64Data)
	if err != nil {
		return "", fmt.Errorf("Base64 解码失败: %w", err)
	}
	decodedHash := sha256.Sum256(decodedData)
	decodedHashStr := hex.EncodeToString(decodedHash[:])
	fmt.Printf("[DEBUG-BF] 解码后数据长度: %d bytes\n", len(decodedData))
	fmt.Printf("[DEBUG-BF] 解码后数据 SHA256: %s\n", decodedHashStr)
	
	if rawHashStr != decodedHashStr {
		return "", fmt.Errorf("Base64 编解码数据不一致! 原始: %s, 解码后: %s", rawHashStr, decodedHashStr)
	}
	fmt.Println("[DEBUG-BF] ✓ Base64 编解码数据一致")
	
	// 再次验证解码后的 BloomFilter
	fmt.Println("[DEBUG-BF] ========== 验证 Base64 解码后的 BloomFilter ==========")
	bf3 := &bloomfilter.BloomFilter{}
	if err := bf3.Unmarshal(decodedData); err != nil {
		return "", fmt.Errorf("Base64 解码后反序列化 BloomFilter 失败: %w", err)
	}
	defer bf3.Free()
	
	testVec3 := vector.NewVec(types.T_varchar.ToType())
	defer testVec3.Free(t.mp)
	
	base64FailCount := 0
	for i, obj := range objects {
		testVec3.Reset(types.T_varchar.ToType())
		if err := vector.AppendBytes(testVec3, []byte(obj), false, t.mp); err != nil {
			fmt.Printf("[DEBUG-BF] ✗ 创建测试 vector 失败: %v\n", err)
			continue
		}
		result := bf3.TestRow(testVec3, 0)
		if result {
			if i < 5 || t.verbose {
				fmt.Printf("[DEBUG-BF] ✓ [%d] Base64解码后 BloomFilter 包含: %s\n", i, obj)
			}
		} else {
			fmt.Printf("[DEBUG-BF] ✗ [%d] Base64解码后 BloomFilter 不包含: %s (这是个问题!)\n", i, obj)
			base64FailCount++
		}
	}
	if base64FailCount > 0 {
		return "", fmt.Errorf("Base64 解码后 BloomFilter 验证失败: %d 个对象未找到", base64FailCount)
	}
	fmt.Printf("[DEBUG-BF] ✓ Base64 解码后验证通过: 所有 %d 个对象都能找到\n", len(objects))
	
	fmt.Println("[DEBUG-BF] ========== BloomFilter 构建完成 ==========")
	fmt.Printf("[DEBUG-BF] 最终 Base64 数据 SHA256: %s\n", rawHashStr)
	
	return base64Data, nil
}

// RegisterProtection 注册保护
func (t *SyncProtectionTester) RegisterProtection(objects []string) error {
	fmt.Println("[DEBUG-REG] ========== 开始注册保护 ==========")
	
	// Build BloomFilter
	bfData, err := t.BuildBloomFilter(objects)
	if err != nil {
		return fmt.Errorf("构建 BloomFilter 失败: %w", err)
	}

	// 计算发送前的 hash
	sendHash := sha256.Sum256([]byte(bfData))
	sendHashStr := hex.EncodeToString(sendHash[:])
	fmt.Printf("[DEBUG-REG] 发送的 Base64 数据 SHA256: %s\n", sendHashStr)
	fmt.Printf("[DEBUG-REG] 发送的 Base64 数据长度: %d\n", len(bfData))

	req := SyncProtectionRequest{
		JobID:   t.jobID,
		BF:      bfData,
		ValidTS: time.Now().UnixNano(),
	}

	jsonData, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("序列化请求失败: %w", err)
	}

	fmt.Printf("[DEBUG-REG] Job ID: %s\n", t.jobID)
	fmt.Printf("[DEBUG-REG] Valid TS: %d\n", req.ValidTS)
	fmt.Printf("[DEBUG-REG] JSON 数据长度: %d\n", len(jsonData))
	
	// 验证 JSON 中的 bf 字段
	var checkReq SyncProtectionRequest
	if err := json.Unmarshal(jsonData, &checkReq); err != nil {
		return fmt.Errorf("JSON 反序列化验证失败: %w", err)
	}
	checkHash := sha256.Sum256([]byte(checkReq.BF))
	checkHashStr := hex.EncodeToString(checkHash[:])
	fmt.Printf("[DEBUG-REG] JSON 中 BF 字段 SHA256: %s\n", checkHashStr)
	if sendHashStr != checkHashStr {
		return fmt.Errorf("JSON 序列化导致 BF 数据变化! 原始: %s, JSON中: %s", sendHashStr, checkHashStr)
	}
	fmt.Println("[DEBUG-REG] ✓ JSON 序列化后 BF 数据一致")

	query := fmt.Sprintf("SELECT mo_ctl('dn', 'diskcleaner', 'register_sync_protection.%s')", string(jsonData))

	fmt.Printf("[DEBUG-REG] SQL 总长度: %d\n", len(query))
	fmt.Printf("[DEBUG-REG] SQL 前200字符: %s...\n", query[:min(200, len(query))])

	var result string
	err = t.db.QueryRow(query).Scan(&result)
	if err != nil {
		return fmt.Errorf("注册保护失败: %w", err)
	}

	fmt.Printf("[DEBUG-REG] MO 返回结果: %s\n", result)

	// 检查是否成功
	if strings.Contains(strings.ToLower(result), "error") {
		return fmt.Errorf("注册保护返回错误: %s", result)
	}

	fmt.Println("[DEBUG-REG] ========== 注册保护完成 ==========")
	fmt.Printf("[DEBUG-REG] 请在 MO 日志中搜索以下关键信息进行对比:\n")
	fmt.Printf("[DEBUG-REG]   - Job ID: %s\n", t.jobID)
	fmt.Printf("[DEBUG-REG]   - BF Base64 SHA256: %s\n", sendHashStr)
	fmt.Printf("[DEBUG-REG]   - BF Base64 长度: %d\n", len(bfData))
	fmt.Printf("[DEBUG-REG]   - 被保护对象数量: %d\n", len(objects))
	fmt.Println("[DEBUG-REG] 被保护的对象列表 (用于与 GC 删除列表对比):")
	for i, obj := range objects {
		fmt.Printf("[DEBUG-REG]   [%d] %s\n", i, obj)
	}

	t.protectedFiles = objects
	return nil
}

// RenewProtection 续租保护
func (t *SyncProtectionTester) RenewProtection() error {
	req := SyncProtectionRequest{
		JobID:   t.jobID,
		ValidTS: time.Now().UnixNano(),
	}

	jsonData, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("序列化请求失败: %w", err)
	}

	query := fmt.Sprintf("SELECT mo_ctl('dn', 'diskcleaner', 'renew_sync_protection.%s')", string(jsonData))

	if t.verbose {
		fmt.Printf("[DEBUG] SQL: %s\n", query)
	}

	var result string
	err = t.db.QueryRow(query).Scan(&result)
	if err != nil {
		return fmt.Errorf("续租保护失败: %w", err)
	}

	if t.verbose {
		fmt.Printf("[DEBUG] 结果: %s\n", result)
	}

	return nil
}

// UnregisterProtection 取消注册保护
func (t *SyncProtectionTester) UnregisterProtection() error {
	req := SyncProtectionRequest{
		JobID: t.jobID,
	}

	jsonData, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("序列化请求失败: %w", err)
	}

	query := fmt.Sprintf("SELECT mo_ctl('dn', 'diskcleaner', 'unregister_sync_protection.%s')", string(jsonData))

	if t.verbose {
		fmt.Printf("[DEBUG] SQL: %s\n", query)
	}

	var result string
	err = t.db.QueryRow(query).Scan(&result)
	if err != nil {
		return fmt.Errorf("取消注册保护失败: %w", err)
	}

	if t.verbose {
		fmt.Printf("[DEBUG] 结果: %s\n", result)
	}

	return nil
}

// TriggerGC 触发 GC
func (t *SyncProtectionTester) TriggerGC() error {
	query := "SELECT mo_ctl('dn', 'diskcleaner', 'force_gc')"

	if t.verbose {
		fmt.Printf("[DEBUG] SQL: %s\n", query)
	}

	var result string
	err := t.db.QueryRow(query).Scan(&result)
	if err != nil {
		return fmt.Errorf("触发 GC 失败: %w", err)
	}

	if t.verbose {
		fmt.Printf("[DEBUG] 结果: %s\n", result)
	}

	return nil
}

// CheckFilesExist 检查文件是否存在
func (t *SyncProtectionTester) CheckFilesExist() (existing, deleted []string) {
	for _, file := range t.protectedFiles {
		// Search for file in data directory
		found := false
		filepath.Walk(t.dataDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if info.Name() == file {
				found = true
				return filepath.SkipAll
			}
			return nil
		})
		if found {
			existing = append(existing, file)
		} else {
			deleted = append(deleted, file)
		}
	}
	return
}

// RunTest 运行测试
func (t *SyncProtectionTester) RunTest() error {
	fmt.Println("========================================")
	fmt.Println("同步保护机制测试 (BloomFilter)")
	fmt.Println("========================================")
	fmt.Printf("Job ID: %s\n", t.jobID)
	fmt.Printf("数据目录: %s\n", t.dataDir)
	fmt.Printf("采样数量: %d\n", t.sampleCount)
	fmt.Printf("等待时间: %d 秒\n", t.waitTime)
	fmt.Println()

	// Step 1: 扫描 object 文件
	fmt.Println("[Step 1] 扫描 object 文件...")
	objects, err := t.ScanObjectFiles()
	if err != nil {
		return err
	}
	fmt.Printf("  找到 %d 个 object 文件\n", len(objects))

	if len(objects) == 0 {
		return fmt.Errorf("没有找到任何 object 文件，请检查数据目录: %s", t.dataDir)
	}

	// Step 2: 随机选择 object
	fmt.Println("[Step 2] 随机选择 object...")
	selected := t.SelectRandomObjects(objects, t.sampleCount)
	fmt.Printf("  选择了 %d 个 object:\n", len(selected))
	for i, obj := range selected {
		if i < 5 {
			fmt.Printf("    - %s\n", obj)
		} else if i == 5 {
			fmt.Printf("    - ... (还有 %d 个)\n", len(selected)-5)
			break
		}
	}

	// Step 3: 构建 BloomFilter 并注册保护
	fmt.Println("[Step 3] 构建 BloomFilter 并注册同步保护...")
	if err := t.RegisterProtection(selected); err != nil {
		return fmt.Errorf("注册保护失败: %w", err)
	}
	fmt.Println("  ✓ 注册成功!")

	// Step 4: 检查文件初始状态
	fmt.Println("[Step 4] 检查文件初始状态...")
	existingBefore, deletedBefore := t.CheckFilesExist()
	fmt.Printf("  存在: %d, 已删除: %d\n", len(existingBefore), len(deletedBefore))

	// Step 5: 触发 GC
	fmt.Println("[Step 5] 触发 GC...")
	if err := t.TriggerGC(); err != nil {
		fmt.Printf("  ⚠ 警告: 触发 GC 失败: %v\n", err)
	} else {
		fmt.Println("  ✓ GC 触发成功!")
	}

	// 等待 GC 完成
	fmt.Printf("[Step 6] 等待 GC 完成 (%d秒)...\n", t.waitTime)
	time.Sleep(time.Duration(t.waitTime) * time.Second)

	// Step 7: 检查文件是否被保护
	fmt.Println("[Step 7] 检查文件保护状态...")
	existingAfter, deletedAfter := t.CheckFilesExist()
	fmt.Printf("  存在: %d, 已删除: %d\n", len(existingAfter), len(deletedAfter))

	// 比较结果
	newlyDeleted := len(deletedAfter) - len(deletedBefore)
	if newlyDeleted > 0 {
		fmt.Printf("  ✗ [失败] 有 %d 个被保护的文件被删除了!\n", newlyDeleted)
		for _, f := range deletedAfter {
			found := false
			for _, bf := range deletedBefore {
				if f == bf {
					found = true
					break
				}
			}
			if !found {
				fmt.Printf("    - 被删除: %s\n", f)
			}
		}
		// 校验失败，停止测试
		return fmt.Errorf("保护机制验证失败：有 %d 个被保护的文件被删除", newlyDeleted)
	} else {
		fmt.Println("  ✓ [成功] 所有被保护的文件都没有被删除!")
	}

	// Step 8: 续租测试
	fmt.Println("[Step 8] 测试续租功能...")
	if err := t.RenewProtection(); err != nil {
		fmt.Printf("  ⚠ 警告: 续租失败: %v\n", err)
	} else {
		fmt.Println("  ✓ 续租成功!")
	}

	// Step 9: 取消注册保护
	fmt.Println("[Step 9] 取消注册保护 (soft delete)...")
	if err := t.UnregisterProtection(); err != nil {
		fmt.Printf("  ⚠ 警告: 取消注册失败: %v\n", err)
	} else {
		fmt.Println("  ✓ 取消注册成功!")
	}

	// Step 10: 再次触发 GC
	fmt.Println("[Step 10] 再次触发 GC...")
	if err := t.TriggerGC(); err != nil {
		fmt.Printf("  ⚠ 警告: 触发 GC 失败: %v\n", err)
	} else {
		fmt.Println("  ✓ GC 触发成功!")
	}

	// 等待 GC 完成
	fmt.Printf("[Step 11] 等待 GC 完成 (%d秒)...\n", t.waitTime)
	time.Sleep(time.Duration(t.waitTime) * time.Second)

	// Step 12: 最终检查
	fmt.Println("[Step 12] 最终检查...")
	existingFinal, deletedFinal := t.CheckFilesExist()
	fmt.Printf("  存在: %d, 已删除: %d\n", len(existingFinal), len(deletedFinal))

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("测试完成!")
	fmt.Println("========================================")

	return nil
}

// PrepareSyncProtectionCommand 准备同步保护测试命令
func PrepareSyncProtectionCommand() *cobra.Command {
	var (
		dsn         string
		dataDir     string
		sampleCount int
		verbose     bool
		waitTime    int
	)

	cmd := &cobra.Command{
		Use:   "sync-protection",
		Short: "测试同步保护机制",
		Long: `测试跨集群同步保护机制。

该命令会：
1. 扫描指定目录获取 object 文件
2. 随机选择一些 object 构建 BloomFilter
3. 注册 BloomFilter 保护
4. 触发 GC 并验证被保护的文件是否被删除
5. 测试续租和取消注册功能`,
		RunE: func(cmd *cobra.Command, args []string) error {
			tester, err := NewSyncProtectionTester(dsn, dataDir, sampleCount, verbose, waitTime)
			if err != nil {
				return err
			}
			defer tester.Close()

			return tester.RunTest()
		},
	}

	cmd.Flags().StringVar(&dsn, "dsn", "root:111@tcp(127.0.0.1:6001)/", "数据库连接字符串")
	cmd.Flags().StringVar(&dataDir, "data-dir", "./mo-data/shared", "数据目录路径")
	cmd.Flags().IntVar(&sampleCount, "sample", 10, "随机采样的 object 数量")
	cmd.Flags().BoolVar(&verbose, "verbose", false, "显示详细输出")
	cmd.Flags().IntVar(&waitTime, "wait", 30, "等待 GC 完成的时间（秒）")

	return cmd
}
