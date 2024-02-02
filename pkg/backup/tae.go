// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backup

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"io"
	"os"
	"path"
	runtime2 "runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	_backupJobPool = sync.Pool{
		New: func() any {
			return new(tasks.Job)
		},
	}
)

func getFileNames(ctx context.Context, retBytes [][][]byte) ([]string, error) {
	var err error
	cr := pb.CtlResult{}
	err = json.Unmarshal(retBytes[0][0], &cr)
	if err != nil {
		return nil, err
	}
	rsSlice, ok := cr.Data.([]interface{})
	if !ok {
		return nil, moerr.NewInternalError(ctx, "invalid ctl result")
	}
	var fileName []string
	for _, rs := range rsSlice {
		str, ok := rs.(string)
		if !ok {
			return nil, moerr.NewInternalError(ctx, "invalid ctl string")
		}

		for _, x := range strings.Split(str, ";") {
			if len(x) == 0 {
				continue
			}
			fileName = append(fileName, x)
		}
	}
	return fileName, err
}

func BackupData(ctx context.Context, srcFs, dstFs fileservice.FileService, dir string, num uint16) error {
	v, ok := runtime.ProcessLevelRuntime().GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		return moerr.NewNotSupported(ctx, "no implement sqlExecutor")
	}
	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}
	sql := "select mo_ctl('dn','checkpoint','')"
	res, err := exec.Exec(ctx, sql, opts)
	if err != nil {
		return err
	}
	sql = "select mo_ctl('dn','Backup','')"
	res, err = exec.Exec(ctx, sql, opts)
	if err != nil {
		return err
	}

	var retByts [][][]byte
	res.ReadRows(func(cols []*vector.Vector) bool {
		retByts = append(retByts, executor.GetBytesRows(cols[0]))
		return true
	})
	res.Close()

	fileName, err := getFileNames(ctx, retByts)
	if err != nil {
		return err
	}
	return execBackup(ctx, srcFs, dstFs, fileName, num)
}

func execBackup(ctx context.Context, srcFs, dstFs fileservice.FileService, names []string, num uint16) error {
	copyTs := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	backupTime := names[0]
	names = names[1:]
	files := make(map[string]objectio.Location, 0)
	table := gc.NewGCTable()
	gcFileMap := make(map[string]string)
	stopPrint := false
	copyCount := 0
	copySize := 0
	var locations []objectio.Location
	var loadDuration, copyDuration, reWriteDuration time.Duration
	cupNum := uint16(runtime2.NumCPU())
	if num < 1 {
		num = 1
	}
	if num > 512 {
		num = 512
	}
	logutil.Info("backup", common.OperationField("start backup"),
		common.AnyField("backup time", backupTime),
		common.AnyField("copy ts ", copyTs.ToString()),
		common.AnyField("checkpoint num", len(names)),
		common.AnyField("cpu num", cupNum),
		common.AnyField("num", num))
	defer func() {
		logutil.Info("backup", common.OperationField("end backup"),
			common.AnyField("load checkpoint cost", loadDuration),
			common.AnyField("copy file cost", copyDuration),
			common.AnyField("rewrite checkpoint cost", reWriteDuration))
	}()
	now := time.Now()
	for _, name := range names {
		if len(name) == 0 {
			continue
		}
		ckpStr := strings.Split(name, ":")
		if len(ckpStr) != 2 {
			return moerr.NewInternalError(ctx, "invalid checkpoint string")
		}
		metaLoc := ckpStr[0]
		version, err := strconv.ParseUint(ckpStr[1], 10, 32)
		if err != nil {
			return err
		}
		key, err := blockio.EncodeLocationFromString(metaLoc)
		if err != nil {
			return err
		}
		loadLocations, data, err := logtail.LoadCheckpointEntriesFromKey(ctx, srcFs, key, uint32(version))
		if err != nil {
			return err
		}
		table.UpdateTable(data)
		gcFiles := table.SoftGC()
		mergeGCFile(gcFiles, gcFileMap)
		locations = append(locations, loadLocations...)
	}
	loadDuration += time.Since(now)
	now = time.Now()
	for _, location := range locations {
		if files[location.Name().String()] == nil {
			/*dentry, err := srcFs.StatFile(ctx, location.Name().String())
			if err != nil {
				if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) &&
					isGC(gcFileMap, location.Name().String()) {
					continue
				} else {
					return err
				}
			}*/
			files[location.Name().String()] = location
		}
	}

	// record files
	taeFileList := make([]*taeFile, 0, len(files))
	jobScheduler := tasks.NewParallelJobScheduler(int(num))
	defer jobScheduler.Stop()
	var fileMutex sync.RWMutex
	var printMutex sync.Mutex
	go func() {
		for {
			printMutex.Lock()
			if stopPrint {
				printMutex.Unlock()
				break
			}
			printMutex.Unlock()
			logutil.Info("backup", common.OperationField("copy file"),
				common.AnyField("copy file size", copySize),
				common.AnyField("copy file num", copyCount),
				common.AnyField("total file num", len(files)))
			time.Sleep(time.Second * 5)
		}
	}()
	now = time.Now()
	backupJobs := make(map[string]*tasks.Job, len(files))
	for n := range files {
		backupJobs[n] = &tasks.Job{}
		backupJobs[n].Init(context.Background(), files[n].String(), tasks.JTAny,
			func(_ context.Context) *tasks.JobResult {

				name := files[n].Name().String()
				size := files[n].Extent().End() + objectio.FooterSize
				checksum, err := CopyFile(context.Background(), srcFs, dstFs, name, int64(size), "")
				if err != nil {
					if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) &&
						isGC(gcFileMap, name) {
						copyCount++
						copySize += int(size)
						return &tasks.JobResult{
							Res: nil,
						}
					} else {
						return &tasks.JobResult{
							Err: err,
							Res: nil,
						}
					}
				}
				fileMutex.Lock()
				copyCount++
				copySize += int(size)
				taeFileList = append(taeFileList, &taeFile{
					path:     name,
					size:     int64(size),
					checksum: checksum,
				})
				fileMutex.Unlock()
				return &tasks.JobResult{
					Res: nil,
				}
			})
		err := jobScheduler.Schedule(backupJobs[n])
		if err != nil {
			logutil.Infof("schedule job failed %v", err.Error())
			return err
		}
	}

	for n := range backupJobs {

		ret := backupJobs[n].WaitDone()
		if ret.Err != nil {
			logutil.Infof("wait job done failed %v", ret.Err.Error())
			return ret.Err
		}
	}
	logutil.Info("backup", common.OperationField("copy file"),
		common.AnyField("copy file size", copySize),
		common.AnyField("copy file num", copyCount),
		common.AnyField("total file num", len(files)))
	printMutex.Lock()
	stopPrint = true
	printMutex.Unlock()
	sizeList, err := CopyDir(ctx, srcFs, dstFs, "ckp", copyTs)
	if err != nil {
		return err
	}
	taeFileList = append(taeFileList, sizeList...)
	sizeList, err = CopyDir(ctx, srcFs, dstFs, "gc", copyTs)
	if err != nil {
		return err
	}
	taeFileList = append(taeFileList, sizeList...)
	copyDuration += time.Since(now)
	//save tae files size
	now = time.Now()
	reWriteDuration += time.Since(now)
	err = saveTaeFilesList(ctx, dstFs, taeFileList, backupTime)
	if err != nil {
		return err
	}
	return nil
}

func CopyDir(ctx context.Context, srcFs, dstFs fileservice.FileService, dir string, backup types.TS) ([]*taeFile, error) {
	var checksum []byte
	files, err := srcFs.List(ctx, dir)
	if err != nil {
		return nil, err
	}
	taeFileList := make([]*taeFile, 0, len(files))

	for _, file := range files {
		if file.IsDir {
			panic("not support dir")
		}
		_, end := blockio.DecodeCheckpointMetadataFileName(file.Name)
		if !backup.IsEmpty() && end.GreaterEq(backup) {
			logutil.Infof("[Backup] skip file %v", file.Name)
			continue
		}
		checksum, err = CopyFile(ctx, srcFs, dstFs, file.Name, file.Size, dir)
		if err != nil {
			return nil, err
		}
		taeFileList = append(taeFileList, &taeFile{
			path:     dir + string(os.PathSeparator) + file.Name,
			size:     file.Size,
			checksum: checksum,
		})
	}
	return taeFileList, nil
}

// CopyFile copy file from srcFs to dstFs and return checksum of the written file.
func CopyFile(ctx context.Context, srcFs, dstFs fileservice.FileService, name string, size int64, dstDir string) ([]byte, error) {
	if dstDir != "" {
		name = path.Join(dstDir, name)
	}
	if size > 1024*1024*1024 {
		logutil.Infof("[Backup] copy big size file %v, %d", name, size)
	}
	var reader io.ReadCloser
	ioVec := &fileservice.IOVector{
		FilePath: name,
		Entries: []fileservice.IOEntry{
			{
				ReadCloserForRead: &reader,
				Offset:            0,
				Size:              -1,
			},
		},
		Policy: fileservice.SkipAllCache,
	}

	err := srcFs.Read(ctx, ioVec)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	// hash
	hasher := sha256.New()
	hashingReader := io.TeeReader(reader, hasher)
	dstIoVec := fileservice.IOVector{
		FilePath: name,
		Entries: []fileservice.IOEntry{
			{
				ReaderForWrite: hashingReader,
				Offset:         0,
				Size:           -1,
			},
		},
		Policy: fileservice.SkipAllCache,
	}

	err = dstFs.Write(ctx, dstIoVec)
	if err != nil {
		return nil, err
	}
	return hasher.Sum(nil), nil
}

func mergeGCFile(gcFiles []string, gcFileMap map[string]string) {
	for _, gcFile := range gcFiles {
		if gcFileMap[gcFile] == "" {
			gcFileMap[gcFile] = gcFile
		}
	}
}

func isGC(gcFileMap map[string]string, name string) bool {
	return gcFileMap[name] != ""
}
