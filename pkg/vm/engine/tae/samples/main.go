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
		vc = append(vc, vec)
	}
	mergesort.SortBlockColumns(vc, 0, test)
	for y := 0; y < 15; y++ {
		vec := vc[y]
		sks := hyperloglog.New()
		containers.ForeachWindowBytes(vec.GetDownstreamVector(), 0, vec.Length(), func(v []byte, isNull bool, row int) (err error) {
			if isNull {
				return
			}
			sks.Insert(v)
			return
		}, nil)
		vec.Close()
	}
}

func main() {
	Transient := dbutils.MakeDefaultTransientPool("trasient-vector-pool")
	for i := 0; i < 20; i++ {
		go Test(Transient)
	}
	for i := 0; i < 20; i++ {
		go Test(Transient)
	}
	for i := 0; i < 20; i++ {
		go Test(Transient)
	}
	for i := 0; i < 20; i++ {
		go Test(Transient)
	}
	for i := 0; i < 20; i++ {
		go Test(Transient)
	}
	time.Sleep(20 * time.Second)
}
