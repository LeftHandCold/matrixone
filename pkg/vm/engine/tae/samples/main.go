package main

import (
	"github.com/axiomhq/hyperloglog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"time"
)

func Test(test *mpool.MPool) {
	for y := 0; y < 30; y++ {
		vecor := objectio.NewStringVector(8192, types.T_varchar.ToType(), test, true, nil)
		vec := containers.ToTNVector(vecor, nil)
		sks := hyperloglog.New()
		containers.ForeachWindowBytes(vec.GetDownstreamVector(), 0, vec.Length(), func(v []byte, isNull bool, row int) (err error) {
			if isNull {
				return
			}
			sks.Insert(v)
			return
		}, nil)
	}
}

func main() {
	test, err := mpool.NewMPool("distributed_tae", 0, mpool.NoFixed)
	if err != nil {
		logutil.Infof("dfsfsdfsdfs")
		return
	}
	go Test(test)
	time.Sleep(20 * time.Second)
}
