// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package engine


import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtags"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/diskmap"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/dustin/go-humanize"
	"github.com/elastic/gosigar"
	"github.com/pkg/errors"
)

// TODO(tamird): why does rocksdb not link jemalloc,snappy statically?

// #cgo CPPFLAGS: -I../../../c-deps/libpmemroach/include
// #cgo LDFLAGS: -lroachpmem
// #cgo LDFLAGS: -lprotobuf
// #cgo LDFLAGS: -lrocksdb
// #cgo linux LDFLAGS: -lrt -lpthread
//
// #include <stdlib.h>
// #include <libpmemroach.h>
import "C"


//export pmemV
func pmemV(sevLvl C.int, infoVerbosity C.int) bool {
	sev := log.Severity(sevLvl)
	return sev == log.Severity_INFO && log.V(int32(infoVerbosity)) ||
		sev == log.Severity_WARNING ||
		sev == log.Severity_ERROR ||
		sev == log.Severity_FATAL
}

//export pmemLog
func pmemLog(sevLvl C.int, s *C.char, n C.int) {
	ctx := logtags.AddTag(context.Background(), "pmem", nil)
	switch log.Severity(sevLvl) {
	case log.Severity_WARNING:
		log.Warning(ctx, C.GoStringN(s, n))
	case log.Severity_ERROR:
		log.Error(ctx, C.GoStringN(s, n))
	case log.Severity_FATAL:
		log.Fatal(ctx, C.GoStringN(s, n))
	default:
		log.Info(ctx, C.GoStringN(s, n))
	}
}

//export prettyPrintPmemKey
func prettyPrintPmemKey(cKey C.DBKey) *C.char {
	mvccKey := MVCCKey{
		Key: gobytes(unsafe.Pointer(cKey.key.data), int(cKey.key.len)),
		Timestamp: hlc.Timestamp{
			WallTime: int64(cKey.wall_time),
			Logical:  int32(cKey.logical),
		},
	}
	return C.CString(mvccKey.String())
}

// RocksDBConfig holds all configuration parameters and knobs used in setting
// up a new RocksDB instance.
type PMemConfig struct {
	Attrs roachpb.Attributes
	// Dir is the data directory for this store.
	Dir string
	// If true, creating the instance fails if the target directory does not hold
	// an initialized RocksDB instance.
	//
	// Makes no sense for in-memory instances.
	MustExist bool
	// ReadOnly will open the database in read only mode if set to true.
	ReadOnly bool
	// MaxSizeBytes is used for calculating free space and making rebalancing
	// decisions. Zero indicates that there is no maximum size.
	MaxSizeBytes int64
	// MaxOpenFiles controls the maximum number of file descriptors RocksDB
	// creates. If MaxOpenFiles is zero, this is set to DefaultMaxOpenFiles.
	MaxOpenFiles uint64
	// WarnLargeBatchThreshold controls if a log message is printed when a
	// WriteBatch takes longer than WarnLargeBatchThreshold. If it is set to
	// zero, no log messages are ever printed.
	WarnLargeBatchThreshold time.Duration
	// Settings instance for cluster-wide knobs.
	Settings *cluster.Settings
	// UseFileRegistry is true if the file registry is needed (eg: encryption-at-rest).
	// This may force the store version to versionFileRegistry if currently lower.
	UseFileRegistry bool
	// RocksDBOptions contains RocksDB specific options using a semicolon
	// separated key-value syntax ("key1=value1; key2=value2").
	RocksDBOptions string
	// ExtraOptions is a serialized protobuf set by Go CCL code and passed through
	// to C CCL code.
	ExtraOptions []byte
}

// RocksDB is a wrapper around a RocksDB database instance.
type PersistentMemoryEngine struct {
	cfg   	PMemConfig
	engine 	*C.PmemEngine

	commit struct {
		syncutil.Mutex
		cond       sync.Cond
		committing bool
		groupSize  int
		pending    []*pmemBatch
	}

	syncer struct {
		syncutil.Mutex
		cond    sync.Cond
		closed  bool
		pending []*pmemBatch
	}

	iters struct {
		syncutil.Mutex
		m map[*pmemIterator][]byte
	}
}

var _ Engine = &PersistentMemoryEngine{}

// NewRocksDB allocates and returns a new RocksDB object.
// This creates options and opens the database. If the database
// doesn't yet exist at the specified directory, one is initialized
// from scratch.
// The caller must call the engine's Close method when the engine is no longer
// needed.
func NewPersistentMemoryEngine(cfg PMemConfig) (*PersistentMemoryEngine, error) {
	if cfg.Dir == "" {
		return nil, errors.New("dir must be non-empty")
	}

	pmem := &PersistentMemoryEngine{
		cfg:   cfg,
	}

	if err := pmem.open(); err != nil {
		return nil, err
	}
	return pmem, nil
}

// String formatter.
func (pmem *PersistentMemoryEngine) String() string {
	attrs := pmem.Attrs().String()
	if attrs == "" {
		attrs = "<no-attributes>"
	}
	return fmt.Sprintf("%s", attrs)
}

// TODO(jeb)
func (pmem *PersistentMemoryEngine) open() error {
	//var existingVersion, newVersion storageVersion
	//if len(pmem.cfg.Dir) != 0 {
	//	log.Infof(context.TODO(), "opening persistent memory instance at %q", pmem.cfg.Dir)
	//
	//	// Check the version number.
	//	var err error
		//if existingVersion, err = getVersion(pmem.cfg.Dir); err != nil {
		//	return err
		//}
	//	if existingVersion < versionMinimum || existingVersion > versionCurrent {
	//		// Instead of an error, we should call a migration if possible when
	//		// one is needed immediately following the DBOpen call.
	//		return fmt.Errorf("incompatible rocksdb data version, current:%d, on disk:%d, minimum:%d",
	//			versionCurrent, existingVersion, versionMinimum)
	//	}
	//
	//	newVersion = existingVersion
	//	if newVersion == versionNoFile {
	//		// We currently set the default store version one before the file registry
	//		// to allow downgrades to older binaries as long as encryption is not in use.
	//		// TODO(mberhault): once enough releases supporting versionFileRegistry have passed, we can upgrade
	//		// to it without worry.
	//		newVersion = versionBeta20160331
	//	}
	//
	//	// Using the file registry forces the latest version. We can't downgrade!
	//	if pmem.cfg.UseFileRegistry {
	//		newVersion = versionCurrent
	//	}
	//} else {
	//	if log.V(2) {
	//		log.Infof(context.TODO(), "opening in memory rocksdb instance")
	//	}

	//	// In memory dbs are always current.
	//	existingVersion = versionCurrent
	//}

	//maxOpenFiles := uint64(RecommendedMaxOpenFiles)
	//if pmem.cfg.MaxOpenFiles != 0 {
	//	maxOpenFiles = pmem.cfg.MaxOpenFiles
	//}

	status := C.DBOpen(&pmem.engine, goToCSlice([]byte(pmem.cfg.Dir)),
		C.DBOptions{
			//cache:             pmem.cache.cache,
			//num_cpu:           C.int(rocksdbConcurrency),
			//max_open_files:    C.int(maxOpenFiles),
			//use_file_registry: C.bool(newVersion == versionCurrent),
			must_exist:        C.bool(r.cfg.MustExist),
			read_only:         C.bool(r.cfg.ReadOnly),
			rocksdb_options:   goToCSlice([]byte(r.cfg.RocksDBOptions)),
			extra_options:     goToCSlice(r.cfg.ExtraOptions),
		})
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not open rocksdb instance")
	}

	// Update or add the version file if needed and if on-disk.
	//if len(pmem.cfg.Dir) != 0 && existingVersion < newVersion {
	//	if err := writeVersionFile(pmem.cfg.Dir, newVersion); err != nil {
	//		return err
	//	}
	//}

	pmem.commit.cond.L = &pmem.commit.Mutex
	pmem.syncer.cond.L = &pmem.syncer.Mutex
	pmem.iters.m = make(map[*pmemIterator][]byte)

	return nil
}

// Close closes the database by deallocating the underlying handle.
func (pmem *PersistentMemoryEngine) Close() {
	if pmem.engine == nil {
		log.Errorf(context.TODO(), "closing unopened persistent memory engine instance")
		return
	}

	log.Infof(context.TODO(), "closing persistent memory engine instance at %q", pmem.cfg.Dir)
	if pmem.engine != nil {
		if err := statusToError(C.DBClose(pmem.engine)); err != nil {
			panic(err)
		}
		pmem.engine = nil
	}
	pmem.syncer.Lock()
	pmem.syncer.closed = true
	pmem.syncer.cond.Signal()
	pmem.syncer.Unlock()
}

func (pmem *PersistentMemoryEngine) CreateCheckpoint(dir string) error {
	return nil
}

// Closed returns true if the engine is closed.
func (pmem *PersistentMemoryEngine) Closed() bool {
	return pmem.engine == nil
}

// Attrs returns the list of attributes describing this engine. This
// may include a specification of disk type (e.g. hdd, ssd, fio, etc.)
// and potentially other labels to identify important attributes of
// the engine.
func (pmem *PersistentMemoryEngine) Attrs() roachpb.Attributes {
	return pmem.cfg.Attrs
}

// Put sets the given key to the value provided.
//
// The key and value byte slices may be reused safely. put takes a copy of
// them before returning.
func (pmem *PersistentMemoryEngine) Put(key MVCCKey, value []byte) error {
	return pmemPut(pmem.engine, key, value)
}

// Merge implements the RocksDB merge operator using the function goMergeInit
// to initialize missing values and goMerge to merge the old and the given
// value into a new value, which is then stored under key.
// Currently 64-bit counter logic is implemented. See the documentation of
// goMerge and goMergeInit for details.
//
// The key and value byte slices may be reused safely. merge takes a copy
// of them before returning.
func (pmem *PersistentMemoryEngine) Merge(key MVCCKey, value []byte) error {
	return pmemMerge(pmem.engine, key, value)
}

// LogData is part of the Writer interface.
func (pmem *PersistentMemoryEngine) LogData(data []byte) error {
	panic("unimplemented")
}

// LogLogicalOp is part of the Writer interface.
func (pmem *PersistentMemoryEngine) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	// No-op. Logical logging disabled.
}

// ApplyBatchRepr atomically applies a set of batched updates. Created by
// calling Repr() on a batch. Using this method is equivalent to constructing
// and committing a batch whose Repr() equals repr.
func (pmem *PersistentMemoryEngine) ApplyBatchRepr(repr []byte, sync bool) error {
	return pmemApplyBatchRepr(pmem.engine, repr, sync)
}

// Get returns the value for the given key.
func (pmem *PersistentMemoryEngine) Get(key MVCCKey) ([]byte, error) {
	return pmemGet(pmem.engine, key)
}

// GetProto fetches the value at the specified key and unmarshals it.
func (pmem *PersistentMemoryEngine) GetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	return pmemGetProto(pmem.engine, key, msg)
}

// Clear removes the item from the db with the given key.
func (pmem *PersistentMemoryEngine) Clear(key MVCCKey) error {
	return pmemClear(pmem.engine, key)
}

// SingleClear removes the most recent item from the db with the given key.
func (pmem *PersistentMemoryEngine) SingleClear(key MVCCKey) error {
	return pmemSingleClear(pmem.engine, key)
}

// ClearRange removes a set of entries, from start (inclusive) to end
// (exclusive).
func (pmem *PersistentMemoryEngine) ClearRange(start, end MVCCKey) error {
	return pmemClearRange(pmem.engine, start, end)
}

// ClearIterRange removes a set of entries, from start (inclusive) to end
// (exclusive).
func (pmem *PersistentMemoryEngine) ClearIterRange(iter Iterator, start, end MVCCKey) error {
	return pmemClearIterRange(pmem.engine, iter, start, end)
}

// Iterate iterates from start to end keys, invoking f on each
// key/value pair. See engine.Iterate for details.
func (pmem *PersistentMemoryEngine) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (bool, error)) error {
	return pmemIterate(pmem.engine, r, start, end, f)
}

// TODO(jeb)
// Capacity queries the underlying file system for disk capacity information.
func (pmem *PersistentMemoryEngine) Capacity() (roachpb.StoreCapacity, error) {
	fileSystemUsage := gosigar.FileSystemUsage{}
	dir := pmem.cfg.Dir
	if dir == "" {
		// This is an in-memory instance. Pretend we're empty since we
		// don't know better and only use this for testing. Using any
		// part of the actual file system here can throw off allocator
		// rebalancing in a hard-to-trace manner. See #7050.
		return roachpb.StoreCapacity{
			Capacity:  pmem.cfg.MaxSizeBytes,
			Available: pmem.cfg.MaxSizeBytes,
		}, nil
	}
	if err := fileSystemUsage.Get(dir); err != nil {
		return roachpb.StoreCapacity{}, err
	}

	if fileSystemUsage.Total > math.MaxInt64 {
		return roachpb.StoreCapacity{}, fmt.Errorf("unsupported disk size %s, max supported size is %s",
			humanize.IBytes(fileSystemUsage.Total), humanizeutil.IBytes(math.MaxInt64))
	}
	if fileSystemUsage.Avail > math.MaxInt64 {
		return roachpb.StoreCapacity{}, fmt.Errorf("unsupported disk size %s, max supported size is %s",
			humanize.IBytes(fileSystemUsage.Avail), humanizeutil.IBytes(math.MaxInt64))
	}
	fsuTotal := int64(fileSystemUsage.Total)
	fsuAvail := int64(fileSystemUsage.Avail)

	// Find the total size of all the files in the pmem.dir and all its
	// subdirectories.
	var totalUsedBytes int64
	if errOuter := filepath.Walk(pmem.cfg.Dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// This can happen if rocksdb removes files out from under us - just keep
			// going to get the best estimate we can.
			if os.IsNotExist(err) {
				return nil
			}
			// Special-case: if the store-dir is configured using the root of some fs,
			// e.g. "/mnt/db", we might have special fs-created files like lost+found
			// that we can't read, so just ignore them rather than crashing.
			if os.IsPermission(err) && filepath.Base(path) == "lost+found" {
				return nil
			}
			return err
		}
		if info.Mode().IsRegular() {
			totalUsedBytes += info.Size()
		}
		return nil
	}); errOuter != nil {
		return roachpb.StoreCapacity{}, errOuter
	}

	// If no size limitation have been placed on the store size or if the
	// limitation is greater than what's available, just return the actual
	// totals.
	if pmem.cfg.MaxSizeBytes == 0 || pmem.cfg.MaxSizeBytes >= fsuTotal || pmem.cfg.Dir == "" {
		return roachpb.StoreCapacity{
			Capacity:  fsuTotal,
			Available: fsuAvail,
			Used:      totalUsedBytes,
		}, nil
	}

	available := pmem.cfg.MaxSizeBytes - totalUsedBytes
	if available > fsuAvail {
		available = fsuAvail
	}
	if available < 0 {
		available = 0
	}

	return roachpb.StoreCapacity{
		Capacity:  pmem.cfg.MaxSizeBytes,
		Available: available,
		Used:      totalUsedBytes,
	}, nil
}

// CompactRange forces compaction over a specified range of keys in the database.
func (pmem *PersistentMemoryEngine) CompactRange(start, end roachpb.Key, forceBottommost bool) error {
	return nil
}

// ApproximateDiskBytes returns the approximate on-disk size of the specified key range.
func (pmem *PersistentMemoryEngine) ApproximateDiskBytes(from, to roachpb.Key) (uint64, error) {
	start := MVCCKey{Key: from}
	end := MVCCKey{Key: to}
	var result C.uint64_t
	err := statusToError(C.PmemApproximateDiskBytes(pmem.engine, goToCKey(start), goToCKey(end), &result))
	return uint64(result), err
}

// Flush causes RocksDB to write all in-memory data to disk immediately.
func (pmem *PersistentMemoryEngine) Flush() error {
	return statusToError(C.DBFlush(pmem.engine))
}

// NewIterator returns an iterator over this rocksdb engine.
func (pmem *PersistentMemoryEngine) NewIterator(opts IterOptions) Iterator {
	return newPmemIterator(pmem.engine, opts, r, r)
}

// NewBatch returns a new batch wrapping this rocksdb engine.
func (pmem *PersistentMemoryEngine) NewBatch() Batch {
	return newPmemBatch(pmem, false /* writeOnly */)
}

// NewWriteOnlyBatch returns a new write-only batch wrapping this rocksdb
// engine.
func (pmem *PersistentMemoryEngine) NewWriteOnlyBatch() Batch {
	return newPmemBatch(pmem, true /* writeOnly */)
}

// GetStats retrieves stats from this engine's RocksDB instance and
// returns it in a new instance of Stats.
func (pmem *PersistentMemoryEngine) GetStats() (*Stats, error) {
	return &Stats{
		BlockCacheHits:                 0,
		BlockCacheMisses:               0,
		BlockCacheUsage:                0,
		BlockCachePinnedUsage:          0,
		BloomFilterPrefixChecked:       0,
		BloomFilterPrefixUseful:        0,
		MemtableTotalSize:              0,
		Flushes:                        0,
		Compactions:                    0,
		TableReadersMemEstimate:        0,
		PendingCompactionBytesEstimate: 0,
		L0FileCount:                    0,
	}, nil
}

// GetEncryptionRegistries returns the file and key registries when encryption is enabled
// on the store.
func (pmem *PersistentMemoryEngine) GetEncryptionRegistries() (*EncryptionRegistries, error) {
	// TODO(jeb) hoping we can fake this
	return nil, errors.New("unsupported")
}

// reusablePmemIterator wraps pmemIterator and allows reuse of an iterator
// for the lifetime of a batch.
type reusablePmemIterator struct {
	pmemIterator
	inuse bool
}

func (r *reusablePmemIterator) Close() {
	// reusableIterator.Close() leaves the underlying rocksdb iterator open until
	// the associated batch is closed.
	if !r.inuse {
		panic("closing idle iterator")
	}
	r.inuse = false
}

type distinctBatch struct {
	*rocksDBBatch
	prefixIter reusablePmemIterator
	normalIter reusablePmemIterator
}

func (r *distinctBatch) Close() {
	if !r.distinctOpen {
		panic("distinct batch not open")
	}
	r.distinctOpen = false
}

// NewIterator returns an iterator over the batch and underlying engine. Note
// that the returned iterator is cached and re-used for the lifetime of the
// batch. A panic will be thrown if multiple prefix or normal (non-prefix)
// iterators are used simultaneously on the same batch.
func (r *distinctBatch) NewIterator(opts IterOptions) Iterator {
	if opts.MinTimestampHint != (hlc.Timestamp{}) {
		// Iterators that specify timestamp bounds cannot be cached.
		if pmem.writeOnly {
			return newRocksDBIterator(r.parent.rdb, opts, r, pmem.parent)
		}
		r.ensureBatch()
		return newRocksDBIterator(r.batch, opts, r, pmem.parent)
	}

	// Use the cached iterator, creating it on first access.
	iter := &r.normalIter
	if opts.Prefix {
		iter = &r.prefixIter
	}
	if iter.rocksDBIterator.iter == nil {
		if pmem.writeOnly {
			iter.rocksDBIterator.init(r.parent.rdb, opts, r, pmem.parent)
		} else {
			r.ensureBatch()
			iter.rocksDBIterator.init(r.batch, opts, r, pmem.parent)
		}
	} else {
		iter.rocksDBIterator.setOptions(opts)
	}
	if iter.inuse {
		panic("iterator already in use")
	}
	iter.inuse = true
	return iter
}

func (r *distinctBatch) Get(key MVCCKey) ([]byte, error) {
	if pmem.writeOnly {
		return dbGet(r.parent.rdb, key)
	}
	r.ensureBatch()
	return dbGet(r.batch, key)
}

func (r *distinctBatch) GetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	if pmem.writeOnly {
		return dbGetProto(r.parent.rdb, key, msg)
	}
	r.ensureBatch()
	return dbGetProto(r.batch, key, msg)
}

func (r *distinctBatch) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (bool, error)) error {
	r.ensureBatch()
	return dbIterate(r.batch, r, start, end, f)
}

func (r *distinctBatch) Put(key MVCCKey, value []byte) error {
	r.builder.Put(key, value)
	return nil
}

func (r *distinctBatch) Merge(key MVCCKey, value []byte) error {
	r.builder.Merge(key, value)
	return nil
}

func (r *distinctBatch) LogData(data []byte) error {
	r.builder.LogData(data)
	return nil
}

func (r *distinctBatch) Clear(key MVCCKey) error {
	r.builder.Clear(key)
	return nil
}

func (r *distinctBatch) SingleClear(key MVCCKey) error {
	r.builder.SingleClear(key)
	return nil
}

func (r *distinctBatch) ClearRange(start, end MVCCKey) error {
	if !r.writeOnly {
		panic("readable batch")
	}
	r.flushMutations()
	r.flushes++ // make sure that Repr() doesn't take a shortcut
	r.ensureBatch()
	return dbClearRange(r.batch, start, end)
}

func (r *distinctBatch) ClearIterRange(iter Iterator, start, end MVCCKey) error {
	r.flushMutations()
	r.flushes++ // make sure that Repr() doesn't take a shortcut
	r.ensureBatch()
	return dbClearIterRange(r.batch, iter, start, end)
}

func (r *distinctBatch) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	// No-op. Logical logging disabled.
}

func (r *distinctBatch) close() {
	if i := &r.prefixIter.rocksDBIterator; i.iter != nil {
		i.destroy()
	}
	if i := &r.normalIter.rocksDBIterator; i.iter != nil {
		i.destroy()
	}
}

// batchIterator wraps rocksDBIterator and ensures that the buffered mutations
// in a batch are flushed before performing read operations.
type batchIterator struct {
	iter  rocksDBIterator
	batch *rocksDBBatch
}

func (r *batchIterator) Stats() IteratorStats {
	return pmem.iter.Stats()
}

func (r *batchIterator) Close() {
	if pmem.batch == nil {
		panic("closing idle iterator")
	}
	r.batch = nil
	r.iter.destroy()
}

func (r *batchIterator) Seek(key MVCCKey) {
	r.batch.flushMutations()
	r.iter.Seek(key)
}

func (r *batchIterator) SeekReverse(key MVCCKey) {
	r.batch.flushMutations()
	r.iter.SeekReverse(key)
}

func (r *batchIterator) Valid() (bool, error) {
	return pmem.iter.Valid()
}

func (r *batchIterator) Next() {
	r.batch.flushMutations()
	r.iter.Next()
}

func (r *batchIterator) Prev() {
	r.batch.flushMutations()
	r.iter.Prev()
}

func (r *batchIterator) NextKey() {
	r.batch.flushMutations()
	r.iter.NextKey()
}

func (r *batchIterator) PrevKey() {
	r.batch.flushMutations()
	r.iter.PrevKey()
}

func (r *batchIterator) ComputeStats(
	start, end MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	r.batch.flushMutations()
	return pmem.iter.ComputeStats(start, end, nowNanos)
}

func (r *batchIterator) FindSplitKey(
	start, end, minSplitKey MVCCKey, targetSize int64,
) (MVCCKey, error) {
	r.batch.flushMutations()
	return pmem.iter.FindSplitKey(start, end, minSplitKey, targetSize)
}

func (r *batchIterator) MVCCGet(
	key roachpb.Key, timestamp hlc.Timestamp, opts MVCCGetOptions,
) (*roachpb.Value, *roachpb.Intent, error) {
	r.batch.flushMutations()
	return pmem.iter.MVCCGet(key, timestamp, opts)
}

func (r *batchIterator) MVCCScan(
	start, end roachpb.Key, max int64, timestamp hlc.Timestamp, opts MVCCScanOptions,
) (kvData []byte, numKVs int64, resumeSpan *roachpb.Span, intents []roachpb.Intent, err error) {
	r.batch.flushMutations()
	return pmem.iter.MVCCScan(start, end, max, timestamp, opts)
}

func (r *batchIterator) SetUpperBound(key roachpb.Key) {
	r.iter.SetUpperBound(key)
}

func (r *batchIterator) Key() MVCCKey {
	return pmem.iter.Key()
}

func (r *batchIterator) Value() []byte {
	return pmem.iter.Value()
}

func (r *batchIterator) ValueProto(msg protoutil.Message) error {
	return pmem.iter.ValueProto(msg)
}

func (r *batchIterator) UnsafeKey() MVCCKey {
	return pmem.iter.UnsafeKey()
}

func (r *batchIterator) UnsafeValue() []byte {
	return pmem.iter.UnsafeValue()
}

func (r *batchIterator) getIter() *C.DBIterator {
	return pmem.iter.iter
}

// reusableBatchIterator wraps batchIterator and makes the Close method a no-op
// to allow reuse of the iterator for the lifetime of the batch. The batch must
// call iter.destroy() when it closes itself.
type reusableBatchIterator struct {
	batchIterator
}

func (r *reusableBatchIterator) Close() {
	// reusableBatchIterator.Close() leaves the underlying rocksdb iterator open
	// until the associated batch is closed.
	if pmem.batch == nil {
		panic("closing idle iterator")
	}
	r.batch = nil
}

type dbIteratorGetter interface {
	getIter() *C.DBIterator
}

type rocksDBIterator struct {
	parent *RocksDB
	engine Reader
	iter   *C.DBIterator
	valid  bool
	reseek bool
	err    error
	key    C.DBKey
	value  C.DBSlice
}

// TODO(peter): Is this pool useful now that rocksDBBatch.NewIterator doesn't
// allocate by returning internal pointers?
var iterPool = sync.Pool{
	New: func() interface{} {
		return &rocksDBIterator{}
	},
}

// newRocksDBIterator returns a new iterator over the supplied RocksDB
// instance. If snapshotHandle is not nil, uses the indicated snapshot.
// The caller must call rocksDBIterator.Close() when finished with the
// iterator to free up resources.
func newRocksDBIterator(
	rdb *C.PmemEngine, opts IterOptions, engine Reader, parent *RocksDB,
) Iterator {
	// In order to prevent content displacement, caching is disabled
	// when performing scans. Any options set within the shared read
	// options field that should be carried over needs to be set here
	// as well.
	r := iterPool.Get().(*rocksDBIterator)
	r.init(rdb, opts, engine, parent)
	return r
}

func (r *rocksDBIterator) getIter() *C.DBIterator {
	return pmem.iter
}

func (r *rocksDBIterator) init(rdb *C.PmemEngine, opts IterOptions, engine Reader, parent *RocksDB) {
	r.parent = parent
	if debugIteratorLeak && pmem.parent != nil {
		r.parent.iters.Lock()
		r.parent.iters.m[r] = debug.Stack()
		r.parent.iters.Unlock()
	}

	if !opts.Prefix && len(opts.UpperBound) == 0 && len(opts.LowerBound) == 0 {
		panic("iterator must set prefix or upper bound or lower bound")
	}

	r.iter = C.DBNewIter(rdb, goToCIterOptions(opts))
	if pmem.iter == nil {
		panic("unable to create iterator")
	}
	r.engine = engine
}

func (r *rocksDBIterator) setOptions(opts IterOptions) {
	if opts.MinTimestampHint != (hlc.Timestamp{}) || opts.MaxTimestampHint != (hlc.Timestamp{}) {
		panic("iterator with timestamp hints cannot be reused")
	}
	if !opts.Prefix && len(opts.UpperBound) == 0 && len(opts.LowerBound) == 0 {
		panic("iterator must set prefix or upper bound or lower bound")
	}
	C.DBIterSetLowerBound(r.iter, goToCKey(MakeMVCCMetadataKey(opts.LowerBound)))
	C.DBIterSetUpperBound(r.iter, goToCKey(MakeMVCCMetadataKey(opts.UpperBound)))
}

func (r *rocksDBIterator) checkEngineOpen() {
	if pmem.engine.Closed() {
		panic("iterator used after backing engine closed")
	}
}

func (r *rocksDBIterator) destroy() {
	if debugIteratorLeak && pmem.parent != nil {
		r.parent.iters.Lock()
		delete(r.parent.iters.m, r)
		r.parent.iters.Unlock()
	}
	C.DBIterDestroy(r.iter)
	*r = rocksDBIterator{}
}

// The following methods implement the Iterator interface.

func (r *rocksDBIterator) Stats() IteratorStats {
	stats := C.DBIterStats(r.iter)
	return IteratorStats{
		TimeBoundNumSSTs:           int(C.ulonglong(stats.timebound_num_ssts)),
		InternalDeleteSkippedCount: int(C.ulonglong(stats.internal_delete_skipped_count)),
	}
}

func (r *rocksDBIterator) Close() {
	r.destroy()
	iterPool.Put(r)
}

func (r *rocksDBIterator) Seek(key MVCCKey) {
	r.checkEngineOpen()
	if len(key.Key) == 0 {
		// start=Key("") needs special treatment since we need
		// to access start[0] in an explicit seek.
		r.setState(C.DBIterSeekToFirst(r.iter))
	} else {
		// We can avoid seeking if we're already at the key we seek.
		if pmem.valid && !r.reseek && key.Equal(r.UnsafeKey()) {
			return
		}
		r.setState(C.DBIterSeek(r.iter, goToCKey(key)))
	}
}

func (r *rocksDBIterator) SeekReverse(key MVCCKey) {
	r.checkEngineOpen()
	if len(key.Key) == 0 {
		r.setState(C.DBIterSeekToLast(r.iter))
	} else {
		// We can avoid seeking if we're already at the key we seek.
		if pmem.valid && !r.reseek && key.Equal(r.UnsafeKey()) {
			return
		}
		r.setState(C.DBIterSeek(r.iter, goToCKey(key)))
		// Maybe the key sorts after the last key in RocksDB.
		if ok, _ := pmem.Valid(); !ok {
			r.setState(C.DBIterSeekToLast(r.iter))
		}
		if ok, _ := pmem.Valid(); !ok {
			return
		}
		// Make sure the current key is <= the provided key.
		if key.Less(r.UnsafeKey()) {
			r.Prev()
		}
	}
}

func (r *rocksDBIterator) Valid() (bool, error) {
	return pmem.valid, pmem.err
}

func (r *rocksDBIterator) Next() {
	r.checkEngineOpen()
	r.setState(C.DBIterNext(r.iter, C.bool(false) /* skip_current_key_versions */))
}

func (r *rocksDBIterator) Prev() {
	r.checkEngineOpen()
	r.setState(C.DBIterPrev(r.iter, C.bool(false) /* skip_current_key_versions */))
}

func (r *rocksDBIterator) NextKey() {
	r.checkEngineOpen()
	r.setState(C.DBIterNext(r.iter, C.bool(true) /* skip_current_key_versions */))
}

func (r *rocksDBIterator) PrevKey() {
	r.checkEngineOpen()
	r.setState(C.DBIterPrev(r.iter, C.bool(true) /* skip_current_key_versions */))
}

func (r *rocksDBIterator) Key() MVCCKey {
	// The data returned by rocksdb_iter_{key,value} is not meant to be
	// freed by the client. It is a direct reference to the data managed
	// by the iterator, so it is copied instead of freed.
	return cToGoKey(r.key)
}

func (r *rocksDBIterator) Value() []byte {
	return cSliceToGoBytes(r.value)
}

func (r *rocksDBIterator) ValueProto(msg protoutil.Message) error {
	if pmem.value.len <= 0 {
		return nil
	}
	return protoutil.Unmarshal(r.UnsafeValue(), msg)
}

func (r *rocksDBIterator) UnsafeKey() MVCCKey {
	return cToUnsafeGoKey(r.key)
}

func (r *rocksDBIterator) UnsafeValue() []byte {
	return cSliceToUnsafeGoBytes(r.value)
}

func (r *rocksDBIterator) clearState() {
	r.valid = false
	r.reseek = true
	r.key = C.DBKey{}
	r.value = C.DBSlice{}
	r.err = nil
}

func (r *rocksDBIterator) setState(state C.DBIterState) {
	r.valid = bool(state.valid)
	r.reseek = false
	r.key = state.key
	r.value = state.value
	r.err = statusToError(state.status)
}

func (r *rocksDBIterator) ComputeStats(
	start, end MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	r.clearState()
	result := C.MVCCComputeStats(r.iter, goToCKey(start), goToCKey(end), C.int64_t(nowNanos))
	stats, err := cStatsToGoStats(result, nowNanos)
	if util.RaceEnabled {
		// If we've come here via batchIterator, then flushMutations (which forces
		// reseek) was called just before C.MVCCComputeStats. Set it here as well
		// to match.
		r.reseek = true
		// C.MVCCComputeStats and ComputeStatsGo must behave identically.
		// There are unit tests to ensure that they return the same result, but
		// as an additional check, use the race builds to check any edge cases
		// that the tests may miss.
		verifyStats, verifyErr := ComputeStatsGo(r, start, end, nowNanos)
		if (err != nil) != (verifyErr != nil) {
			panic(fmt.Sprintf("C.MVCCComputeStats differed from ComputeStatsGo: err %v vs %v", err, verifyErr))
		}
		if !stats.Equal(verifyStats) {
			panic(fmt.Sprintf("C.MVCCComputeStats differed from ComputeStatsGo: stats %+v vs %+v", stats, verifyStats))
		}
	}
	return stats, err
}

func (r *rocksDBIterator) FindSplitKey(
	start, end, minSplitKey MVCCKey, targetSize int64,
) (MVCCKey, error) {
	var splitKey C.DBString
	r.clearState()
	status := C.MVCCFindSplitKey(r.iter, goToCKey(start), goToCKey(end), goToCKey(minSplitKey),
		C.int64_t(targetSize), &splitKey)
	if err := statusToError(status); err != nil {
		return MVCCKey{}, err
	}
	return MVCCKey{Key: cStringToGoBytes(splitKey)}, nil
}

func (r *rocksDBIterator) MVCCGet(
	key roachpb.Key, timestamp hlc.Timestamp, opts MVCCGetOptions,
) (*roachpb.Value, *roachpb.Intent, error) {
	if opts.Inconsistent && opts.Txn != nil {
		return nil, nil, errors.Errorf("cannot allow inconsistent reads within a transaction")
	}
	if len(key) == 0 {
		return nil, nil, emptyKeyError()
	}

	r.clearState()
	state := C.MVCCGet(
		r.iter, goToCSlice(key), goToCTimestamp(timestamp), goToCTxn(opts.Txn),
		C.bool(opts.Inconsistent), C.bool(opts.Tombstones), C.bool(opts.IgnoreSequence),
	)

	if err := statusToError(state.status); err != nil {
		return nil, nil, err
	}
	if err := uncertaintyToError(timestamp, state.uncertainty_timestamp, opts.Txn); err != nil {
		return nil, nil, err
	}

	intents, err := buildScanIntents(cSliceToGoBytes(state.intents))
	if err != nil {
		return nil, nil, err
	}
	if !opts.Inconsistent && len(intents) > 0 {
		return nil, nil, &roachpb.WriteIntentError{Intents: intents}
	}

	var intent *roachpb.Intent
	if len(intents) > 1 {
		return nil, nil, errors.Errorf("expected 0 or 1 intents, got %d", len(intents))
	} else if len(intents) == 1 {
		intent = &intents[0]
	}
	if state.data.len == 0 {
		return nil, intent, nil
	}

	count := state.data.count
	if count > 1 {
		return nil, nil, errors.Errorf("expected 0 or 1 result, found %d", count)
	}
	if count == 0 {
		return nil, intent, nil
	}

	// Extract the value from the batch data.
	repr := copyFromSliceVector(state.data.bufs, state.data.len)
	mvccKey, rawValue, _, err := MVCCScanDecodeKeyValue(repr)
	if err != nil {
		return nil, nil, err
	}
	value := &roachpb.Value{
		RawBytes:  rawValue,
		Timestamp: mvccKey.Timestamp,
	}
	return value, intent, nil
}

func (r *rocksDBIterator) MVCCScan(
	start, end roachpb.Key, max int64, timestamp hlc.Timestamp, opts MVCCScanOptions,
) (kvData []byte, numKVs int64, resumeSpan *roachpb.Span, intents []roachpb.Intent, err error) {
	if opts.Inconsistent && opts.Txn != nil {
		return nil, 0, nil, nil, errors.Errorf("cannot allow inconsistent reads within a transaction")
	}
	if len(end) == 0 {
		return nil, 0, nil, nil, emptyKeyError()
	}
	if max == 0 {
		resumeSpan = &roachpb.Span{Key: start, EndKey: end}
		return nil, 0, resumeSpan, nil, nil
	}

	r.clearState()
	state := C.MVCCScan(
		r.iter, goToCSlice(start), goToCSlice(end),
		goToCTimestamp(timestamp), C.int64_t(max),
		goToCTxn(opts.Txn), C.bool(opts.Inconsistent),
		C.bool(opts.Reverse), C.bool(opts.Tombstones),
		C.bool(opts.IgnoreSequence),
	)

	if err := statusToError(state.status); err != nil {
		return nil, 0, nil, nil, err
	}
	if err := uncertaintyToError(timestamp, state.uncertainty_timestamp, opts.Txn); err != nil {
		return nil, 0, nil, nil, err
	}

	kvData = copyFromSliceVector(state.data.bufs, state.data.len)
	numKVs = int64(state.data.count)

	if resumeKey := cSliceToGoBytes(state.resume_key); resumeKey != nil {
		if opts.Reverse {
			resumeSpan = &roachpb.Span{Key: start, EndKey: roachpb.Key(resumeKey).Next()}
		} else {
			resumeSpan = &roachpb.Span{Key: resumeKey, EndKey: end}
		}
	}

	intents, err = buildScanIntents(cSliceToGoBytes(state.intents))
	if err != nil {
		return nil, 0, nil, nil, err
	}
	if !opts.Inconsistent && len(intents) > 0 {
		// When encountering intents during a consistent scan we still need to
		// return the resume key.
		return nil, 0, resumeSpan, nil, &roachpb.WriteIntentError{Intents: intents}
	}

	return kvData, numKVs, resumeSpan, intents, nil
}

func (r *rocksDBIterator) SetUpperBound(key roachpb.Key) {
	C.DBIterSetUpperBound(r.iter, goToCKey(MakeMVCCMetadataKey(key)))
}

func copyFromSliceVector(bufs *C.DBSlice, len C.int32_t) []byte {
	if bufs == nil {
		return nil
	}

	// Interpret the C pointer as a pointer to a Go array, then slice.
	slices := (*[1 << 20]C.DBSlice)(unsafe.Pointer(bufs))[:len:len]
	neededBytes := 0
	for i := range slices {
		neededBytes += int(slices[i].len)
	}
	data := nonZeroingMakeByteSlice(neededBytes)[:0]
	for i := range slices {
		data = append(data, cSliceToUnsafeGoBytes(slices[i])...)
	}
	return data
}

func cStatsToGoStats(stats C.MVCCStatsResult, nowNanos int64) (enginepb.MVCCStats, error) {
	ms := enginepb.MVCCStats{}
	if err := statusToError(stats.status); err != nil {
		return ms, err
	}
	ms.ContainsEstimates = false
	ms.LiveBytes = int64(stats.live_bytes)
	ms.KeyBytes = int64(stats.key_bytes)
	ms.ValBytes = int64(stats.val_bytes)
	ms.IntentBytes = int64(stats.intent_bytes)
	ms.LiveCount = int64(stats.live_count)
	ms.KeyCount = int64(stats.key_count)
	ms.ValCount = int64(stats.val_count)
	ms.IntentCount = int64(stats.intent_count)
	ms.IntentAge = int64(stats.intent_age)
	ms.GCBytesAge = int64(stats.gc_bytes_age)
	ms.SysBytes = int64(stats.sys_bytes)
	ms.SysCount = int64(stats.sys_count)
	ms.LastUpdateNanos = nowNanos
	return ms, nil
}

// goToCSlice converts a go byte slice to a DBSlice. Note that this is
// potentially dangerous as the DBSlice holds a reference to the go
// byte slice memory that the Go GC does not know about. This method
// is only intended for use in converting arguments to C
// functions. The C function must copy any data that it wishes to
// retain once the function returns.
func goToCSlice(b []byte) C.DBSlice {
	if len(b) == 0 {
		return C.DBSlice{data: nil, len: 0}
	}
	return C.DBSlice{
		data: (*C.char)(unsafe.Pointer(&b[0])),
		len:  C.int(len(b)),
	}
}

func goToCKey(key MVCCKey) C.DBKey {
	return C.DBKey{
		key:       goToCSlice(key.Key),
		wall_time: C.int64_t(key.Timestamp.WallTime),
		logical:   C.int32_t(key.Timestamp.Logical),
	}
}

func cToGoKey(key C.DBKey) MVCCKey {
	// When converting a C.DBKey to an MVCCKey, give the underlying slice an
	// extra byte of capacity in anticipation of roachpb.Key.Next() being
	// called. The extra byte is trivial extra space, but allows callers to avoid
	// an allocation and copy when calling roachpb.Key.Next(). Note that it is
	// important that the extra byte contain the value 0 in order for the
	// roachpb.Key.Next() fast-path to be invoked. This is true for the code
	// below because make() zero initializes all of the bytes.
	unsafeKey := cSliceToUnsafeGoBytes(key.key)
	safeKey := make([]byte, len(unsafeKey), len(unsafeKey)+1)
	copy(safeKey, unsafeKey)

	return MVCCKey{
		Key: safeKey,
		Timestamp: hlc.Timestamp{
			WallTime: int64(key.wall_time),
			Logical:  int32(key.logical),
		},
	}
}

func cToUnsafeGoKey(key C.DBKey) MVCCKey {
	return MVCCKey{
		Key: cSliceToUnsafeGoBytes(key.key),
		Timestamp: hlc.Timestamp{
			WallTime: int64(key.wall_time),
			Logical:  int32(key.logical),
		},
	}
}

func cStringToGoString(s C.DBString) string {
	if s.data == nil {
		return ""
	}
	result := C.GoStringN(s.data, s.len)
	C.free(unsafe.Pointer(s.data))
	return result
}

func cStringToGoBytes(s C.DBString) []byte {
	if s.data == nil {
		return nil
	}
	result := gobytes(unsafe.Pointer(s.data), int(s.len))
	C.free(unsafe.Pointer(s.data))
	return result
}

func cSliceToGoBytes(s C.DBSlice) []byte {
	if s.data == nil {
		return nil
	}
	return gobytes(unsafe.Pointer(s.data), int(s.len))
}

func cSliceToUnsafeGoBytes(s C.DBSlice) []byte {
	if s.data == nil {
		return nil
	}
	// Interpret the C pointer as a pointer to a Go array, then slice.
	return (*[maxArrayLen]byte)(unsafe.Pointer(s.data))[:s.len:s.len]
}

func goToCTimestamp(ts hlc.Timestamp) C.DBTimestamp {
	return C.DBTimestamp{
		wall_time: C.int64_t(ts.WallTime),
		logical:   C.int32_t(ts.Logical),
	}
}

func goToCTxn(txn *roachpb.Transaction) C.DBTxn {
	var r C.DBTxn
	if txn != nil {
		r.id = goToCSlice(txn.ID.GetBytes())
		r.epoch = C.uint32_t(txn.Epoch)
		r.sequence = C.int32_t(txn.Sequence)
		r.max_timestamp = goToCTimestamp(txn.MaxTimestamp)
	}
	return r
}

func goToCIterOptions(opts IterOptions) C.DBIterOptions {
	return C.DBIterOptions{
		prefix:             C.bool(opts.Prefix),
		lower_bound:        goToCKey(MakeMVCCMetadataKey(opts.LowerBound)),
		upper_bound:        goToCKey(MakeMVCCMetadataKey(opts.UpperBound)),
		min_timestamp_hint: goToCTimestamp(opts.MinTimestampHint),
		max_timestamp_hint: goToCTimestamp(opts.MaxTimestampHint),
		with_stats:         C.bool(opts.WithStats),
	}
}

func statusToError(s C.DBStatus) error {
	if s.data == nil {
		return nil
	}
	return &RocksDBError{msg: cStringToGoString(s)}
}

func uncertaintyToError(
	readTS hlc.Timestamp, existingTS C.DBTimestamp, txn *roachpb.Transaction,
) error {
	if existingTS.wall_time != 0 || existingTS.logical != 0 {
		return roachpb.NewReadWithinUncertaintyIntervalError(
			readTS, hlc.Timestamp{
				WallTime: int64(existingTS.wall_time),
				Logical:  int32(existingTS.logical),
			},
			txn)
	}
	return nil
}

// goMerge takes existing and update byte slices that are expected to
// be marshaled roachpb.Values and merges the two values returning a
// marshaled roachpb.Value or an error.
func goMerge(existing, update []byte) ([]byte, error) {
	var result C.DBString
	status := C.DBMergeOne(goToCSlice(existing), goToCSlice(update), &result)
	if status.data != nil {
		return nil, errors.Errorf("%s: existing=%q, update=%q",
			cStringToGoString(status), existing, update)
	}
	return cStringToGoBytes(result), nil
}

// goPartialMerge takes existing and update byte slices that are expected to
// be marshaled roachpb.Values and performs a partial merge using C++ code,
// marshaled roachpb.Value or an error.
func goPartialMerge(existing, update []byte) ([]byte, error) {
	var result C.DBString
	status := C.DBPartialMergeOne(goToCSlice(existing), goToCSlice(update), &result)
	if status.data != nil {
		return nil, errors.Errorf("%s: existing=%q, update=%q",
			cStringToGoString(status), existing, update)
	}
	return cStringToGoBytes(result), nil
}

func emptyKeyError() error {
	return errors.Errorf("attempted access to empty key")
}

func pmemPut(rdb *C.PmemEngine, key MVCCKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	// *Put, *Get, and *Delete call memcpy() (by way of MemTable::Add)
	// when called, so we do not need to worry about these byte slices
	// being reclaimed by the GC.
	return statusToError(C.PmemPut(rdb, goToCKey(key), goToCSlice(value)))
}

func pmemMerge(rdb *C.PmemEngine, key MVCCKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	// DBMerge calls memcpy() (by way of MemTable::Add)
	// when called, so we do not need to worry about these byte slices being
	// reclaimed by the GC.
	return statusToError(C.PmemMerge(rdb, goToCKey(key), goToCSlice(value)))
}

func pmemApplyBatchRepr(rdb *C.PmemEngine, repr []byte, sync bool) error {
	return statusToError(C.PmemApplyBatchRepr(rdb, goToCSlice(repr), C.bool(sync)))
}

// dbGet returns the value for the given key.
func pmemGet(rdb *C.PmemEngine, key MVCCKey) ([]byte, error) {
	if len(key.Key) == 0 {
		return nil, emptyKeyError()
	}
	var result C.DBString
	err := statusToError(C.PmemGet(rdb, goToCKey(key), &result))
	if err != nil {
		return nil, err
	}
	return cStringToGoBytes(result), nil
}

func pmemGetProto(
	rdb *C.PmemEngine, key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	if len(key.Key) == 0 {
		err = emptyKeyError()
		return
	}
	var result C.DBString
	if err = statusToError(C.PmemGet(rdb, goToCKey(key), &result)); err != nil {
		return
	}
	if result.len <= 0 {
		msg.Reset()
		return
	}
	ok = true
	if msg != nil {
		// Make a byte slice that is backed by result.data. This slice
		// cannot live past the lifetime of this method, but we're only
		// using it to unmarshal the roachpb.
		data := cSliceToUnsafeGoBytes(C.DBSlice(result))
		err = protoutil.Unmarshal(data, msg)
	}
	C.free(unsafe.Pointer(result.data))
	keyBytes = int64(key.EncodedSize())
	valBytes = int64(result.len)
	return
}

func pmemClear(rdb *C.PmemEngine, key MVCCKey) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return statusToError(C.PmemDelete(rdb, goToCKey(key)))
}

func pmemSingleClear(rdb *C.PmemEngine, key MVCCKey) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return statusToError(C.PmemSingleDelete(rdb, goToCKey(key)))
}

func pmemClearRange(rdb *C.PmemEngine, start, end MVCCKey) error {
	if err := statusToError(C.PmemDeleteRange(rdb, goToCKey(start), goToCKey(end))); err != nil {
		return err
	}

	// TODO(JEB) should i care here?
	// This is a serious hack. RocksDB generates sstables which cover an
	// excessively large amount of the key space when range tombstones are
	// present. The crux of the problem is that the logic for determining sstable
	// boundaries depends on actual keys being present. So we help that logic
	// along by adding deletions of the first key covered by the range tombstone,
	// and a key near the end of the range (previous is difficult). See
	// TestRocksDBDeleteRangeCompaction which verifies that either this hack is
	// working, or the upstream problem was fixed in RocksDB.
	if err := dbClear(rdb, start); err != nil {
		return err
	}
	prev := make(roachpb.Key, len(end.Key))
	copy(prev, end.Key)
	if n := len(prev) - 1; prev[n] > 0 {
		prev[n]--
	} else {
		prev = prev[:n]
	}
	if start.Key.Compare(prev) < 0 {
		if err := pmemClear(rdb, MakeMVCCMetadataKey(prev)); err != nil {
			return err
		}
	}
	return nil
}

func pmemClearIterRange(rdb *C.PmemEngine, iter Iterator, start, end MVCCKey) error {
	getter, ok := iter.(dbIteratorGetter)
	if !ok {
		return errors.Errorf("%T is not a RocksDB iterator", iter)
	}
	return statusToError(C.PmemDeleteIterRange(rdb, getter.getIter(), goToCKey(start), goToCKey(end)))
}

func pmemIterate(
	rdb *C.PmemEngine, engine Reader, start, end MVCCKey, f func(MVCCKeyValue) (bool, error),
) error {
	if !start.Less(end) {
		return nil
	}
	it := newPmemIterator(rdb, IterOptions{UpperBound: end.Key}, engine, nil)
	defer it.Close()

	it.Seek(start)
	for ; ; it.Next() {
		ok, err := it.Valid()
		if err != nil {
			return err
		} else if !ok {
			break
		}
		k := it.Key()
		if !k.Less(end) {
			break
		}
		if done, err := f(MVCCKeyValue{Key: k, Value: it.Value()}); done || err != nil {
			return err
		}
	}
	return nil
}

func (pmem *PersistentMemoryEngine) PreIngestDelay(ctx context.Context) {
}

// WriteFile writes data to a file in this RocksDB's env.
func (pmem *PersistentMemoryEngine) WriteFile(filename string, data []byte) error {
	return statusToError(C.DBEnvWriteFile(pmem.engine, goToCSlice([]byte(filename)), goToCSlice(data)))
}

// OpenFile opens a DBFile, which is essentially a rocksdb WritableFile
// with the given filename, in this RocksDB's env.
func (pmem *PersistentMemoryEngine) OpenFile(filename string) (DBFile, error) {
	return nil, errors.New("not supported in PoC")
}

// ReadFile reads the content from a file with the given filename. The file
// must have been opened through Engine.OpenFile. Otherwise an error will be
// returned.
func (pmem *PersistentMemoryEngine) ReadFile(filename string) ([]byte, error) {
	return nil, errors.New("not supported in PoC")
}

// DeleteFile deletes the file with the given filename from this RocksDB's env.
// If the file with given filename doesn't exist, return os.ErrNotExist.
func (pmem *PersistentMemoryEngine) DeleteFile(filename string) error {
	return errors.New("not supported in PoC")
}

// DeleteDirAndFiles deletes the directory and any files it contains but
// not subdirectories from this RocksDB's env. If dir does not exist,
// DeleteDirAndFiles returns nil (no error).
func (pmem *PersistentMemoryEngine) DeleteDirAndFiles(dir string) error {
	return errors.New("not supported in PoC")
}

// LinkFile creates 'newname' as a hard link to 'oldname'. This use the Env responsible for the file
// which may handle extra logic (eg: copy encryption settings for EncryptedEnv).
func (pmem *PersistentMemoryEngine) LinkFile(oldname, newname string) error {
	return errors.New("not supported in PoC")
}

// NewSortedDiskMap implements the MapProvidingEngine interface.
func (pmem *PersistentMemoryEngine) NewSortedDiskMap() diskmap.SortedDiskMap {
	return NewRocksDBMap(r)
}

// NewSortedDiskMultiMap implements the MapProvidingEngine interface.
func (pmem *PersistentMemoryEngine) NewSortedDiskMultiMap() diskmap.SortedDiskMap {
	return NewRocksDBMultiMap(r)
}

// IsValidSplitKey returns whether the key is a valid split key. Certain key
// ranges cannot be split (the meta1 span and the system DB span); split keys
// chosen within any of these ranges are considered invalid. And a split key
// equal to Meta2KeyMax (\x03\xff\xff) is considered invalid.
func IsValidSplitKey(key roachpb.Key) bool {
	return bool(C.MVCCIsValidSplitKey(goToCSlice(key)))
}

// lockFile sets a lock on the specified file using RocksDB's file locking interface.
func lockFile(filename string) (C.DBFileLock, error) {
	var lock C.DBFileLock
	// C.DBLockFile mutates its argument. `lock, statusToError(...)`
	// happens to work in gc, but does not work in gccgo.
	//
	// See https://github.com/golang/go/issues/23188.
	err := statusToError(C.DBLockFile(goToCSlice([]byte(filename)), &lock))
	return lock, err
}

// unlockFile unlocks the file asscoiated with the specified lock and GCs any allocated memory for the lock.
func unlockFile(lock C.DBFileLock) error {
	return statusToError(C.DBUnlockFile(lock))
}

// MVCCScanDecodeKeyValue decodes a key/value pair returned in an MVCCScan
// "batch" (this is not the RocksDB batch repr format), returning both the
// key/value and the suffix of data remaining in the batch.
func MVCCScanDecodeKeyValue(repr []byte) (key MVCCKey, value []byte, orepr []byte, err error) {
	k, ts, value, orepr, err := enginepb.ScanDecodeKeyValue(repr)
	return MVCCKey{k, ts}, value, orepr, err
}

func notFoundErrOrDefault(err error) error {
	errStr := err.Error()
	if strings.Contains(errStr, "No such file or directory") ||
		strings.Contains(errStr, "File not found") ||
		strings.Contains(errStr, "The system cannot find the path specified") {
		return os.ErrNotExist
	}
	return err
}

func (pmem *PersistentMemoryEngine) GetAuxiliaryDir() string {
	panic("implement me")
}

func (pmem *PersistentMemoryEngine) NewReadOnly() ReadWriter {
	panic("implement me")
}

func (pmem *PersistentMemoryEngine) NewSnapshot() Reader {
	panic("implement me")
}

func (pmem *PersistentMemoryEngine) IngestExternalFiles(ctx context.Context, paths []string, skipWritingSeqNo, allowFileModifications bool) error {
	panic("implement me")
}
