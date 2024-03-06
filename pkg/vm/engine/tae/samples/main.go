package main

import (
	"github.com/axiomhq/hyperloglog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"time"
)

func Test(test *containers.VectorPool) {
	vc := make([]containers.Vector, 0)
	for i := 0; i < 15; i++ {
		vecor := objectio.NewStringVector(8192, types.T_varchar.ToType(), test.GetAllocator(), true, nil)
		vec := containers.ToTNVector(vecor, test.GetAllocator())
		ve := vec.CloneWindowWithPool(0, vec.Length(), test)
		vc = append(vc, ve)
	}
	mergesort.SortBlockColumns(vc, 0, test)
	for _, v1 := range vc {
		sks := hyperloglog.New()
		containers.ForeachWindowBytes(v1.GetDownstreamVector(), 0, v1.Length(), func(v []byte, isNull bool, row int) (err error) {
			if isNull {
				return
			}
			sks.Insert(v)
			return
		}, nil)
	}

	for _, v1 := range vc {
		v1.Close()
	}
}

func main() {
	Transient := dbutils.MakeDefaultTransientPool("trasient-vector-pool")
	for i := 0; i < 20; i++ {
		Test(Transient)
	}
	for i := 0; i < 20; i++ {
		Test(Transient)
	}
	for i := 0; i < 20; i++ {
		Test(Transient)
	}
	for i := 0; i < 20; i++ {
		Test(Transient)
	}
	for i := 0; i < 20; i++ {
		Test(Transient)
	}
	time.Sleep(20 * time.Second)
}
