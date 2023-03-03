package dataio

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

// Reader is the only interface that mo provides for CN/DN/ETL... modules to read data
type Reader interface {
	// LoadColumns loads the data of a block specified column
	// idxs is the column number of the data to be read
	// ids is the block id to read.
	// if idxs is nil, read data for all columns
	// extent is the address offset information of all block metadata
	// stored in an object, which is obtained after calling Writer.Sync
	// extent.id can find a specific block in an object, extent is not nil
	LoadColumns(ctx context.Context, idxs []uint16, ids []uint32, m *mpool.MPool) ([]*batch.Batch, error)

	// LoadZoneMaps loads the ZoneMap index of the specified column of the block
	// Returns a two-dimensional array of ZoneMap data structures
	LoadZoneMaps(ctx context.Context, idxs []uint16, ids []uint32, m *mpool.MPool) ([][]*index.ZoneMap, error)

	// LoadBloomFilter loads the BloomFilter index of the specified column of the block
	// idx is the column number of the index to be read,Only one column of data in a block has BloomFilter
	// ids is the block id to read.
	// Returns index.StaticFilter data structures
	LoadBloomFilter(ctx context.Context, idx uint16, ids []uint32, m *mpool.MPool) ([]index.StaticFilter, error)

	// LoadColumnsByTS loads the data of the column specified by the block at a certain point in time(ts)
	// info is the BlockInfo of the block, including MetaLoc/DeltaLoc/CommitTs... some
	// protocol information, which needs to be processed internally and returned to the caller's visible data
	LoadColumnsByTS(ctx context.Context, idxs []uint16, info catalog.BlockInfo,
		ts timestamp.Timestamp, m *mpool.MPool) (*batch.Batch, error)

	LoadBlocksMeta(ctx context.Context, m *mpool.MPool) ([]objectio.BlockObject, error)
}

// Writer is the only interface that mo provides to CN/DN/ETL... modules to write data
// default no primary key，a buffer will be created in the Writer to store the written batch
type Writer interface {
	// SetPrimaryKey Set the primary key of the writer, because the written
	// data needs to generate BloomFilter according to the primary key
	SetPrimaryKey(idx uint16)

	// WriteBatch writes a batch into the buffer, and at the same time
	// generates a ZoneMap for each column in the batch, and generates
	// a BloomFilter for the primary key if there is a primary key, and
	// these indexes are also written into the buffer
	// Returns metadata of a block (BlockObject)
	WriteBatch(batch *batch.Batch) (objectio.BlockObject, error)

	// WriteBatchWithOutIndex writes a batch into the buffer without generating any index
	WriteBatchWithOutIndex(batch *batch.Batch) (objectio.BlockObject, error)

	// Sync is to write multiple batches written to the buffer to the fileservice at one time
	// objectio.Extent is the address offset information of all block metadata stored in an object.
	Sync(ctx context.Context) ([]objectio.BlockObject, objectio.Extent, error)
}
