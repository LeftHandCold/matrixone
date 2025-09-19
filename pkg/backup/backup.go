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
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
)

// Backup
// Note: ctx needs to support cancel. The user can cancel the backup task by canceling the ctx.
func Backup(
	ctx context.Context,
	sid string,
	bs *tree.BackupStart,
	cfg *Config,
) error {
	var err error
	var s3Conf *s3Config
	if !cfg.metasMustBeSet() {
		return moerr.NewInternalError(ctx, "invalid config or metas or fileservice")
	}
	if bs == nil {
		return moerr.NewInternalError(ctx, "invalid backup start")
	}
	// step 1 : setup fileservice
	//1.1 setup ETL fileservice for general usage
	if !bs.IsS3 {
		cfg.GeneralDir, _, err = setupFilesystem(ctx, bs.Dir, true)
		if err != nil {
			return err
		}
		// for tae hakeeper
		cfg.TaeDir, _, err = setupFilesystem(ctx, bs.Dir, false)
		if err != nil {
			return err
		}
		// for parallel backup
		parallel, err := strconv.ParseUint(bs.Parallelism, 10, 16)
		if err != nil {
			return err
		}
		cfg.Parallelism = uint16(parallel)
	} else {
		s3Conf, err = getS3Config(ctx, bs.Option)
		if err != nil {
			return err
		}
		cfg.GeneralDir, _, err = setupS3(ctx, s3Conf, true)
		if err != nil {
			return err
		}
		cfg.TaeDir, _, err = setupS3(ctx, s3Conf, false)
		if err != nil {
			return err
		}
		cfg.Parallelism = s3Conf.parallelism
	}

	if bs.BackupTs == "" {
		cfg.BackupTs = types.TS{}
	} else {
		cfg.BackupTs = types.StringToTS(bs.BackupTs)
	}
	cfg.BackupType = bs.BackupType

	// step 2 : setup backup protection
	backupID := uuid.New().String()
	protectedPaths := []string{"shared/ckp", "shared/gc", ""} // protect checkpoint, gc metadata and all data files

	// Add backup protection
	if err = addBackupProtection(ctx, sid, backupID, cfg.BackupTs, protectedPaths); err != nil {
		logutil.Warn("failed to add backup protection", zap.Error(err))
		// Continue with backup even if protection setup fails
	}

	// Setup heartbeat mechanism
	heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
	defer heartbeatCancel()

	go func() {
		ticker := time.NewTicker(2 * time.Minute) // heartbeat every 2 minutes
		defer ticker.Stop()

		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				if err := updateBackupHeartbeat(ctx, sid, backupID); err != nil {
					logutil.Warn("failed to update backup heartbeat",
						zap.String("backup_id", backupID),
						zap.Error(err))
				}
			}
		}
	}()

	// Ensure cleanup on exit
	defer func() {
		heartbeatCancel()
		if cleanupErr := removeBackupProtection(context.Background(), sid, backupID); cleanupErr != nil {
			logutil.Warn("failed to remove backup protection",
				zap.String("backup_id", backupID),
				zap.Error(cleanupErr))
		}
	}()

	// step 3 : backup mo
	if err = backupBuildInfo(ctx, cfg); err != nil {
		return err
	}

	if err = backupConfigs(ctx, cfg); err != nil {
		return err
	}

	if err = backupTae(ctx, sid, cfg); err != nil {
		return err
	}

	if err = backupHakeeper(ctx, cfg); err != nil {
		return err
	}

	if err = saveMetas(ctx, cfg); err != nil {
		return err
	}

	return err
}

// saveBuildInfo saves backupVersion, build info.
func backupBuildInfo(ctx context.Context, cfg *Config) error {
	cfg.Metas.AppendVersion(Version)
	cfg.Metas.AppendBuildinfo(buildInfo())
	return nil
}

// saveConfigs saves cluster config or service config
func backupConfigs(ctx context.Context, cfg *Config) error {
	var err error
	// save cluster config files
	for typ, files := range launchConfigPaths {
		for _, f := range files {
			err = backupConfigFile(ctx, typ, f, cfg)
			if err != nil {
				return err
			}
		}
	}

	return err
}

var backupTae = func(
	ctx context.Context,
	sid string,
	config *Config,
) error {
	fs := fileservice.SubPath(config.TaeDir, taeDir)
	return BackupData(ctx, sid, config.SharedFs, fs, "", config)
}

func backupHakeeper(ctx context.Context, config *Config) error {
	var (
		err    error
		haData []byte
	)
	if !config.metasGeneralFsMustBeSet() {
		return moerr.NewInternalError(ctx, "invalid config or metas or fileservice")
	}
	if config.HAkeeper == nil {
		return moerr.NewInternalError(ctx, "hakeeper client is nil")
	}
	fs := fileservice.SubPath(config.TaeDir, hakeeperDir)
	// get hakeeper data
	haData, err = config.HAkeeper.GetBackupData(ctx)
	if err != nil {
		return err
	}
	return writeFile(ctx, fs, HakeeperFile, haData)
}

func backupConfigFile(ctx context.Context, typ, configPath string, cfg *Config) error {
	if !cfg.metasGeneralFsMustBeSet() {
		return moerr.NewInternalError(ctx, "invalid config or metas or fileservice")
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		logutil.Errorf("read file %s failed, err: %v", configPath, err)
		//!!!neglect the error
		return nil
	}
	uid, _ := uuid.NewV7()
	_, file := path.Split(configPath)
	newfile := file + "_" + uid.String()
	cfg.Metas.AppendLaunchconfig(typ, newfile)
	filename := configDir + "/" + newfile
	return writeFile(ctx, cfg.GeneralDir, filename, data)
}

func saveMetas(ctx context.Context, cfg *Config) error {
	if !cfg.metasGeneralFsMustBeSet() {
		return moerr.NewInternalError(ctx, "invalid config or metas or fileservice")
	}
	lines := cfg.Metas.CsvString()
	metas, err := ToCsvLine2(lines)
	if err != nil {
		return err
	}
	return writeFile(ctx, cfg.GeneralDir, moMeta, []byte(metas))
}

func ToCsvLine2(s [][]string) (string, error) {
	ss := strings.Builder{}
	writer := csv.NewWriter(&ss)
	for _, t := range s {
		err := writer.Write(t)
		if err != nil {
			return "", err
		}
	}

	writer.Flush()
	return ss.String(), nil
}

func saveTaeFilesList(ctx context.Context, Fs fileservice.FileService, taeFiles []*taeFile, backupTime, backupTS, typ string) error {
	var err error
	if Fs == nil {
		return moerr.NewInternalError(ctx, "fileservice is nil")
	}
	_, err = time.Parse(time.DateTime, backupTime)
	if err != nil {
		return err
	}
	lines, size := taeFileListToCsv(taeFiles)
	metas, err := ToCsvLine2(lines)
	if err != nil {
		return err
	}
	//save tae files list
	err = writeFile(ctx, Fs, taeList, []byte(metas))
	if err != nil {
		return err
	}

	//save tae files size
	lines = [][]string{taeBackupTimeAndSizeToCsv(backupTime, backupTS, typ, size)}
	metas, err = ToCsvLine2(lines)
	if err != nil {
		return err
	}

	return writeFile(ctx, Fs, taeSum, []byte(metas))
}

// fromCsvBytes converts the csv bytes to the array of string
func fromCsvBytes(data []byte) ([][]string, error) {
	r := csv.NewReader(bytes.NewReader(data))
	return r.ReadAll()
}

// addBackupProtection adds backup protection via mo_ctl
func addBackupProtection(ctx context.Context, sid string, backupID string, backupTS types.TS, protectedPaths []string) error {
	v, ok := runtime.ServiceRuntime(sid).GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		return moerr.NewNotSupported(ctx, "no implement sqlExecutor")
	}

	exec := v.(executor.SQLExecutor)

	protectionReq := map[string]interface{}{
		"action":          "add",
		"backup_id":       backupID,
		"backup_ts":       backupTS.ToString(),
		"protected_paths": protectedPaths,
	}

	reqJSON, err := json.Marshal(protectionReq)
	if err != nil {
		return err
	}

	sql := "select mo_ctl('dn','BACKUP_PROTECTION','" + string(reqJSON) + "')"
	opts := executor.Options{}
	res, err := exec.Exec(ctx, sql, opts)
	if err != nil {
		return err
	}
	res.Close()

	logutil.Info("backup protection added",
		zap.String("backup_id", backupID),
		zap.String("backup_ts", backupTS.ToString()),
	)

	return nil
}

// updateBackupHeartbeat updates backup protection heartbeat via mo_ctl
func updateBackupHeartbeat(ctx context.Context, sid string, backupID string) error {
	v, ok := runtime.ServiceRuntime(sid).GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		return moerr.NewNotSupported(ctx, "no implement sqlExecutor")
	}

	exec := v.(executor.SQLExecutor)

	heartbeatReq := map[string]interface{}{
		"action":    "heartbeat",
		"backup_id": backupID,
	}

	reqJSON, err := json.Marshal(heartbeatReq)
	if err != nil {
		return err
	}

	sql := "select mo_ctl('dn','BACKUP_PROTECTION','" + string(reqJSON) + "')"
	opts := executor.Options{}
	res, err := exec.Exec(ctx, sql, opts)
	if err != nil {
		return err
	}
	res.Close()

	return nil
}

// removeBackupProtection removes backup protection via mo_ctl
func removeBackupProtection(ctx context.Context, sid string, backupID string) error {
	v, ok := runtime.ServiceRuntime(sid).GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		return moerr.NewNotSupported(ctx, "no implement sqlExecutor")
	}

	exec := v.(executor.SQLExecutor)

	removeReq := map[string]interface{}{
		"action":    "remove",
		"backup_id": backupID,
	}

	reqJSON, err := json.Marshal(removeReq)
	if err != nil {
		return err
	}

	sql := "select mo_ctl('dn','BACKUP_PROTECTION','" + string(reqJSON) + "')"
	opts := executor.Options{}
	res, err := exec.Exec(ctx, sql, opts)
	if err != nil {
		return err
	}
	res.Close()

	logutil.Info("backup protection removed",
		zap.String("backup_id", backupID),
	)

	return nil
}
