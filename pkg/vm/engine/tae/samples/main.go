package main

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/cespare/xxhash/v2"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fifocache"
)

var gcache *fifocache.Cache[string, []byte]

func init() {
	go http.ListenAndServe("0.0.0.0:6060", nil)
	gcache = fifocache.New[string, []byte](mpool.GB, nil, shardMetaCacheKey)
}

func shardMetaCacheKey(v string) uint8 {
	key := []byte(v)
	return uint8(xxhash.Sum64(key[:]))
}

func doCache(str1 string) {
	for i := 0; i < 2000; i++ {
		gcache.Set(fmt.Sprintf("%s", i), []byte(str1), len(str1))
	}
	fmt.Println("done")
	//gcache.Reset()
	runtime.GC()
}

func main() {
	str1 := strings.Repeat("xxxx", 1024*1024*1)
	go doCache(str1)
	for i := 0; i < 1000; i++ {
		time.Sleep(time.Second * 10)
	}
}
