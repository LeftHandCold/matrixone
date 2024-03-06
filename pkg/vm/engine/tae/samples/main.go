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

func Test() {
	time.Sleep(2 * time.Second)
	test, err := mpool.NewMPool("distributed_tae", 0, mpool.NoFixed)
	if err != nil {
		logutil.Infof("dfsfsdfsdfs")
		return
	}
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
	testi := []byte{18, 70, 87, 115, 89, 80, 72, 84, 86, 106, 116, 80, 105, 73, 78, 81, 48, 67, 85, 0, 0, 0, 0, 0}
	var v types.Varlena
	logutil.Infof("test1 is %v", testi)
	vlen := len(testi)
	copy(v[0:vlen], testi)
	tes := v.GetByteSlice(nil)
	logutil.Infof("tes is %v", tes)
	sks.Insert(tes)
}

func main() {

	Test()
}
