package main

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/migrate"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc/v3"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/spf13/cobra"
)

type migrateArg struct {
}

func (c *migrateArg) PrepareCommand() *cobra.Command {
	migrateCmd := &cobra.Command{
		Use:   "migrate",
		Short: "migrate ckp",
		Run:   RunFactory(c),
	}

	replay := replayArg{}
	migrateCmd.AddCommand(replay.PrepareCommand())

	gc := gcArg{}
	migrateCmd.AddCommand(gc.PrepareCommand())

	rollback := rollbackArg{}
	migrateCmd.AddCommand(rollback.PrepareCommand())

	test := testArg{}
	migrateCmd.AddCommand(test.PrepareCommand())

	backup := backupArg{}
	migrateCmd.AddCommand(backup.PrepareCommand())

	return migrateCmd
}

func (c *migrateArg) FromCommand(cmd *cobra.Command) (err error) {
	return nil
}

func (c *migrateArg) String() string {
	return ""
}

func (c *migrateArg) Run() error {
	return nil
}

func getFsArg(input string) (arg migrate.FSArg, err error) {
	var data []byte
	if data, err = os.ReadFile(input); err != nil {
		return
	}
	if err = jsoniter.Unmarshal(data, &arg); err != nil {
		return
	}
	return
}

type replayArg struct {
	arg       migrate.FSArg
	cfg, meta string
	rootDir   string
	tid       uint64
}

func (c *replayArg) PrepareCommand() *cobra.Command {
	replayCmd := &cobra.Command{
		Use:   "replay",
		Short: "replay ckp",
		Run:   RunFactory(c),
	}

	replayCmd.Flags().StringP("input", "i", "", "input")
	replayCmd.Flags().StringP("root", "r", "", "root")
	replayCmd.Flags().Uint64P("tid", "t", 0, "tid")

	return replayCmd
}

func (c *replayArg) FromCommand(cmd *cobra.Command) (err error) {
	c.rootDir = cmd.Flag("root").Value.String()
	cfg := cmd.Flag("input").Value.String()
	if c.rootDir == "" {
		c.arg, err = getFsArg(cfg)
		if err != nil {
			panic(err)
		}
	}
	c.tid, _ = cmd.Flags().GetUint64("tid")
	return nil
}

func (c *replayArg) String() string {
	return ""
}

const (
	ckpDir    = "ckp"
	ckpBakDir = "ckp-bak"
	gcDir     = "gc"
	gcBakDir  = "gc-bak"

	oldObjDir   = "rewritten/old"
	newObjDir   = "rewritten/new"
	rollbackDir = "rewritten/rollback"
)

func cleanDir(fs fileservice.FileService, dir string) {
	ctx := context.Background()
	entries, _ := fileservice.SortedList(fs.List(ctx, dir))
	for _, entry := range entries {
		err := fs.Delete(ctx, dir+"/"+entry.Name)
		if err != nil {
			logutil.Infof("delete %s/%s failed", dir, entry.Name)
		}
	}
}

const (
	rootDir = "/home/mo/wenbin/matrixone/mo-data"
)

func getLatestCkpMeta(fs fileservice.FileService, dir string) (res string) {
	dirs, _ := fileservice.SortedList(fs.List(context.Background(), dir))
	var name string
	maxTs := types.BuildTS(0, 0)
	minTs := types.BuildTS(0, 0)
	for _, dir := range dirs {
		start, end, _ := blockio.DecodeCheckpointMetadataFileName(dir.Name)
		if start.EQ(&minTs) && end.GT(&maxTs) {
			maxTs = end
			name = dir.Name
		}
	}
	return name
}

func (c *replayArg) Run() error {
	blockio.Start("")
	defer blockio.Stop("")

	var dataFS, oldObjFS, newObjFS, rollbackFS fileservice.FileService

	ctx := context.Background()
	if c.rootDir != "" { // just for local test
		dataFS = migrate.NewFileFs(ctx, c.rootDir)
		oldObjFS = migrate.NewFileFs(ctx, path.Join(c.rootDir, oldObjDir))
		newObjFS = migrate.NewFileFs(ctx, path.Join(c.rootDir, newObjDir))
		rollbackFS = migrate.NewFileFs(ctx, path.Join(c.rootDir, rollbackDir))
	} else {
		dataFS = migrate.NewS3FsWithCache(ctx, c.arg)
		oldObjFS = migrate.NewS3Fs(ctx, c.arg, oldObjDir)
		newObjFS = migrate.NewS3Fs(ctx, c.arg, newObjDir)
		rollbackFS = migrate.NewS3Fs(ctx, c.arg, rollbackDir)
	}

	c.meta = getLatestCkpMeta(dataFS, ckpDir)

	now := time.Now()
	start := time.Now()
	// Backup ckp meta files
	migrate.BackupCkpDir(ctx, dataFS, ckpDir, ckpBakDir)
	migrate.BackupCkpDir(ctx, dataFS, gcDir, gcBakDir)
	logutil.Infof("[duration] backup ckp files done, cost %v, total %v", time.Since(start), time.Since(now))

	// ListCkpFiles
	start = time.Now()
	migrate.GetCkpFiles(ctx, dataFS, oldObjFS)
	logutil.Infof("[duration] dump old ckp files done, cost %v, total %v", time.Since(start), time.Since(now))

	// ReadCkp11File
	start = time.Now()
	fromEntry, ckpbats := migrate.ReadCkp11File(ctx, dataFS, filepath.Join(ckpBakDir, c.meta))

	// Replay To 1.3 catalog
	cata := migrate.ReplayCatalogFromCkpData11(ckpbats)

	//dbIt := cata.MakeDBIt(false)
	//for ; dbIt.Valid(); dbIt.Next() {
	//	dbEntry := dbIt.Get().GetPayload()
	//	tblIt := dbEntry.MakeTableIt(false)
	//	for ; tblIt.Valid(); tblIt.Next() {
	//		tblEntry := tblIt.Get().GetPayload()
	//		fmt.Println(dbEntry.GetFullName(), tblEntry.GetFullName(), tblEntry.GetID())
	//	}
	//}

	// Dump catalog to 3 tables batch
	bDb, bTbl, bCol, snapshotMeta := migrate.DumpCatalogToBatches(cata)

	// Sink and get object stats
	objDB := migrate.SinkBatch(ctx, catalog.SystemDBSchema, bDb, dataFS)
	objTbl := migrate.SinkBatch(ctx, catalog.SystemTableSchema, bTbl, dataFS)
	objCol := migrate.SinkBatch(ctx, catalog.SystemColumnSchema, bCol, dataFS)

	{
		rollbackSinker := migrate.NewSinker(migrate.ObjectListSchema, rollbackFS)
		defer rollbackSinker.Close()
		var ss []objectio.ObjectStats
		ss = append(ss, objDB...)
		ss = append(ss, objTbl...)
		ss = append(ss, objCol...)

		migrate.SinkObjectBatch(ctx, rollbackSinker, ss)
	}

	logutil.Infof("[duration] replay catalog done, cost %v, total %v", time.Since(start), time.Since(now))

	// Clean ckp and gc dir
	cleanDir(dataFS, ckpDir)
	cleanDir(dataFS, gcDir)

	// Write 1.3 Global Ckp
	start = time.Now()
	txnNode := &txnbase.TxnMVCCNode{
		Start:   types.BuildTS(42424242, 0),
		Prepare: types.BuildTS(42424243, 0),
		End:     types.BuildTS(42424243, 0),
	}
	entryNode := &catalog.EntryMVCCNode{
		CreatedAt: types.BuildTS(42424243, 0),
	}

	migrate.RewriteCkp(ctx, cata, dataFS, newObjFS, rollbackFS,
		fromEntry, ckpbats, txnNode, entryNode, objDB, objTbl, objCol, c.tid)

	logutil.Infof("[duration] rewrite ckp done, cost %v, total %v", time.Since(start), time.Since(now))

	for _, v := range objDB {
		println(v.String())
	}
	for _, v := range objTbl {
		println(v.String())
	}
	for _, v := range objCol {
		println(v.String())
	}
	name := blockio.EncodeTableMetadataFileName(
		gc.PrefixAcctMeta,
		fromEntry.GetStart(),
		fromEntry.GetEnd(),
	)
	_, err := snapshotMeta.SaveTableInfo(gc.GCMetaDir+name, dataFS)
	if err != nil {
		println(err.Error())
		return err
	}
	return nil
}

type gcArg struct {
	rootDir string
	arg     migrate.FSArg
}

func (c *gcArg) PrepareCommand() *cobra.Command {
	gcCmd := &cobra.Command{
		Use:   "gc",
		Short: "gc checkpoint files",
		Run:   RunFactory(c),
	}

	gcCmd.Flags().StringP("root", "r", "", "root")
	gcCmd.Flags().StringP("input", "i", "", "input")

	return gcCmd
}

func (c *gcArg) FromCommand(cmd *cobra.Command) (err error) {
	c.rootDir = cmd.Flag("root").Value.String()
	cfg := cmd.Flag("input").Value.String()
	if c.rootDir == "" {
		c.arg, err = getFsArg(cfg)
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func (c *gcArg) String() string {
	return ""
}

func (c *gcArg) Run() error {
	blockio.Start("")
	defer blockio.Stop("")

	ctx := context.Background()
	var fs fileservice.FileService
	if c.rootDir != "" { // just for local test
		fs = migrate.NewFileFs(ctx, c.rootDir)
	} else {
		fs = migrate.NewS3FsWithCache(ctx, c.arg)
	}

	migrate.GcCheckpointFiles(ctx, fs)

	return nil
}

type rollbackArg struct {
	rootDir string
	arg     migrate.FSArg
}

func (c *rollbackArg) PrepareCommand() *cobra.Command {
	gcCmd := &cobra.Command{
		Use:   "rollback",
		Short: "rollback changes",
		Run:   RunFactory(c),
	}

	gcCmd.Flags().StringP("root", "r", "", "root")
	gcCmd.Flags().StringP("input", "i", "", "input")

	return gcCmd
}

func (c *rollbackArg) FromCommand(cmd *cobra.Command) (err error) {
	c.rootDir = cmd.Flag("root").Value.String()
	cfg := cmd.Flag("input").Value.String()
	if c.rootDir == "" {
		c.arg, err = getFsArg(cfg)
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func (c *rollbackArg) String() string {
	return ""
}

func (c *rollbackArg) Run() error {
	blockio.Start("")
	defer blockio.Stop("")

	ctx := context.Background()
	var fs fileservice.FileService
	if c.rootDir != "" { // just for local test
		fs = migrate.NewFileFs(ctx, c.rootDir)
	} else {
		fs = migrate.NewS3FsWithCache(ctx, c.arg)
	}

	migrate.Rollback(ctx, fs)

	return nil
}

type backupArg struct {
	arg migrate.FSArg
}

func (c *backupArg) PrepareCommand() *cobra.Command {
	testCmd := &cobra.Command{
		Use:   "backup",
		Short: "backup ckp and gc",
		Run:   RunFactory(c),
	}

	testCmd.Flags().StringP("input", "i", "", "input")

	return testCmd
}

func (c *backupArg) FromCommand(cmd *cobra.Command) (err error) {
	cfg := cmd.Flag("input").Value.String()
	c.arg, err = getFsArg(cfg)
	if err != nil {
		panic(err)
	}
	return nil
}

func (c *backupArg) String() string {
	return ""
}

func (c *backupArg) Run() error {
	blockio.Start("")
	defer blockio.Stop("")

	ctx := context.Background()
	fs := migrate.NewS3FsWithCache(ctx, c.arg)
	migrate.BackupCkpDir(ctx, fs, ckpDir, filepath.Join("backup", ckpDir))
	migrate.BackupCkpDir(ctx, fs, gcDir, filepath.Join("backup", gcDir))

	return nil
}

type testArg struct {
	arg migrate.FSArg
}

func (c *testArg) PrepareCommand() *cobra.Command {
	testCmd := &cobra.Command{
		Use:   "test",
		Short: "test s3 fs",
		Run:   RunFactory(c),
	}

	testCmd.Flags().StringP("input", "i", "", "input")

	return testCmd
}

func (c *testArg) FromCommand(cmd *cobra.Command) (err error) {
	cfg := cmd.Flag("input").Value.String()
	c.arg, err = getFsArg(cfg)
	if err != nil {
		panic(err)
	}
	return nil
}

func (c *testArg) String() string {
	return ""
}

func (c *testArg) Run() error {
	blockio.Start("")
	defer blockio.Stop("")

	ctx := context.Background()
	fs := migrate.NewS3FsWithCache(ctx, c.arg)

	entries, err := fileservice.SortedList(fs.List(ctx, ckpDir))
	if err != nil {
		panic(err)
	}

	for _, entry := range entries {
		println(entry.Name, entry.IsDir, entry.Size)
	}

	return nil
}
