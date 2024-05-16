package check_gc

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	gc2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc"
)

func initFS() fileservice.FileService {
	fsDir := "/Users/shenjiangwei/Work/local/tae/matrixone/mo-data/shared"
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: fsDir,
	}
	ctx := context.Background()
	service, err := fileservice.NewFileService(ctx, c, nil)
	if err != nil {
		panic(err)
	}
	return service
}

func main() {
	service := initFS()
	fs := objectio.NewObjectFS(service, "serviceDir")
	cleaner := gc2.NewCheckpointCleaner(context.Background(), fs, nil, true)
	cleaner.Replay()
	inputs := cleaner.GetInputs()
	inputs.GetAllObject()
}
