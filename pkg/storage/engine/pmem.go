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
	"io/ioutil"
	"os"
	"sync"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/diskmap"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
)

// #cgo CPPFLAGS: -I../../../c-deps/libpmemroach/include
// #cgo LDFLAGS: -lpmemroach
// #cgo LDFLAGS: -lprotobuf
// #cgo linux LDFLAGS: -lrt -lpthread
//
// #include <stdlib.h>
// #include <libpmemroach.h>
import "C"
// include the libroach headers for simple things like DBStatus,  DBSlice, DBKey


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
func prettyPrintPmemKey(cKey C.PmemKey) *C.char {
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
	// PmemOptions contains RocksDB specific options using a semicolon
	// separated key-value syntax ("key1=value1; key2=value2").
	PmemOptions string
	// ExtraOptions is a serialized protobuf set by Go CCL code and passed through
	// to C CCL code.
	ExtraOptions []byte
}

// RocksDB is a wrapper around a RocksDB database instance.
type PersistentMemoryEngine struct {
	cfg   	PMemConfig
	engine 	*C.PmemEngine
	auxDir string

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

	// set up the auxillary directory. pretty sure we don't need it for pmem PoC, but just in case...
	auxDir, err := ioutil.TempDir(os.TempDir(), "cockroach-pmem-auxiliary")
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(auxDir, 0755); err != nil {
		return nil, err
	}
	pmem.auxDir = auxDir

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

	status := C.PmemOpen(&pmem.engine, goToCPmemSlice([]byte(pmem.cfg.Dir)),
		C.PmemOptions{
			//num_cpu:           C.int(rocksdbConcurrency),
			must_exist:        C.bool(pmem.cfg.MustExist),
			read_only:         C.bool(pmem.cfg.ReadOnly),
			rocksdb_options:   goToCPmemSlice([]byte(pmem.cfg.PmemOptions)),
			extra_options:     goToCPmemSlice(pmem.cfg.ExtraOptions),
		})
	if err := pmemStatusToError(status); err != nil {
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
		if err := pmemStatusToError(C.PmemClose(pmem.engine)); err != nil {
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

// Merge implements the RocksDB merge operator using the function goPmemMergeInit
// to initialize missing values and goPmemMerge to merge the old and the given
// value into a new value, which is then stored under key.
// Currently 64-bit counter logic is implemented. See the documentation of
// goPmemMerge and goPmemMergeInit for details.
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
	return pmemIterate(pmem.engine, pmem, start, end, f)
}

// TODO(jeb) fill me in with accurate info from the pmem -- might be
// Capacity queries the underlying file system for disk capacity information.
func (pmem *PersistentMemoryEngine) Capacity() (roachpb.StoreCapacity, error) {

	return roachpb.StoreCapacity{
		Capacity:  pmem.cfg.MaxSizeBytes,
		Available: pmem.cfg.MaxSizeBytes,
	}, nil
}

func (pmem *PersistentMemoryEngine) CompactRange(start, end roachpb.Key, forceBottommost bool) error {
	return nil
}

// ApproximateDiskBytes returns the approximate on-disk size of the specified key range.
func (pmem *PersistentMemoryEngine) ApproximateDiskBytes(from, to roachpb.Key) (uint64, error) {
	start := MVCCKey{Key: from}
	end := MVCCKey{Key: to}
	var result C.uint64_t
	err := pmemStatusToError(C.PmemApproximateDiskBytes(pmem.engine, goToCPmemKey(start), goToCPmemKey(end), &result))
	return uint64(result), err
}

func (pmem *PersistentMemoryEngine) Flush() error {
	// nop in pmem-land
	return nil
}

// NewIterator returns an iterator over this pmem engine.
func (pmem *PersistentMemoryEngine) NewIterator(opts IterOptions) Iterator {
	return newPmemIterator(pmem.engine, opts, pmem, pmem)
}

// NewBatch returns a new batch wrapping this pmem engine.
func (pmem *PersistentMemoryEngine) NewBatch() Batch {
	return newPmemBatch(pmem, false /* writeOnly */)
}

// NewWriteOnlyBatch returns a new write-only batch wrapping this pmem
// engine.
func (pmem *PersistentMemoryEngine) NewWriteOnlyBatch() Batch {
	return newPmemBatch(pmem, true /* writeOnly */)
}

// GetStats retrieves stats from this engine's pmem instance and
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

// GetEnvStats returns stats for the RocksDB env. This may include encryption stats.
func (pmem *PersistentMemoryEngine) GetEnvStats() (*EnvStats, error) {
	return &EnvStats{
		TotalFiles:       0,
		TotalBytes:       0,
		ActiveKeyFiles:   0,
		ActiveKeyBytes:   0,
		EncryptionType:   0,
		EncryptionStatus: nil,
	}, nil
}

func (pmem *PersistentMemoryEngine) GetAuxiliaryDir() string {
	return pmem.auxDir
}

// GetEncryptionRegistries returns the file and key registries when encryption is enabled
// on the store.
func (pmem *PersistentMemoryEngine) GetEncryptionRegistries() (*EncryptionRegistries, error) {
	// TODO(jeb) hoping we can fake this
	return nil, errors.New("unsupported")
}

// TODO(jeb) is this necessary?
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
	if iter.pmemIterator.iter == nil {
		if pmem.writeOnly {
			iter.pmemIterator.init(r.parent.rdb, opts, r, pmem.parent)
		} else {
			r.ensureBatch()
			iter.pmemIterator.init(r.batch, opts, r, pmem.parent)
		}
	} else {
		iter.pmemIterator.setOptions(opts)
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
	if i := &r.prefixIter.pmemIterator; i.iter != nil {
		i.destroy()
	}
	if i := &r.normalIter.pmemIterator; i.iter != nil {
		i.destroy()
	}
}

// batchIterator wraps pmemIterator and ensures that the buffered mutations
// in a batch are flushed before performing read operations.
type batchIterator struct {
	iter  pmemIterator
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

func (r *batchIterator) getIter() *C.PmemIterator {
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

// TODO(jeb) wtf is this?!?!?
type dbIteratorGetter interface {
	getIter() *C.PmemIterator
}

type pmemIterator struct {
	parent *PersistentMemoryEngine
	engine Reader
	// TODO(jeb) is has yet to be seen if we need a magical iterator from the pmem impl,
	//  unlike what we need from rocks
	iter   *C.PmemIterator
	valid  bool
	reseek bool
	err    error
	key    C.PmemKey
	value  C.PmemSlice
}

// newRocksDBIterator returns a new iterator over the supplied RocksDB
// instance. If snapshotHandle is not nil, uses the indicated snapshot.
// The caller must call pmemIterator.Close() when finished with the
// iterator to free up resources.
func newPmemIterator(
	pmem *C.PmemEngine, opts IterOptions, engine Reader, parent *PersistentMemoryEngine,
) Iterator {
	it := &pmemIterator{}
	it.init(pmem, opts, engine, parent)
	return it
}

func (it *pmemIterator) getIter() *C.PmemIterator {
	return it.iter
}

func (it *pmemIterator) init(pmem *C.PmemEngine, opts IterOptions, engine Reader, parent *PersistentMemoryEngine) {
	it.parent = parent

	if !opts.Prefix && len(opts.UpperBound) == 0 && len(opts.LowerBound) == 0 {
		panic("iterator must set prefix or upper bound or lower bound")
	}

	it.iter = C.PmemNewIter(pmem, goToCPmemIterOptions(opts))
	if pmem.iter == nil {
		panic("unable to create iterator")
	}
	it.engine = engine
}

func (it *pmemIterator) setOptions(opts IterOptions) {
	if opts.MinTimestampHint != (hlc.Timestamp{}) || opts.MaxTimestampHint != (hlc.Timestamp{}) {
		panic("iterator with timestamp hints cannot be reused")
	}
	if !opts.Prefix && len(opts.UpperBound) == 0 && len(opts.LowerBound) == 0 {
		panic("iterator must set prefix or upper bound or lower bound")
	}
	C.PmemIterSetLowerBound(it.iter, goToCPmemKey(MakeMVCCMetadataKey(opts.LowerBound)))
	C.PmemIterSetUpperBound(it.iter, goToCPmemKey(MakeMVCCMetadataKey(opts.UpperBound)))
}

func (it *pmemIterator) checkEngineOpen() {
	if it.engine.Closed() {
		panic("iterator used after backing engine closed")
	}
}

func (it *pmemIterator) destroy() {
	C.PmemIterDestroy(it.iter)
	*it = pmemIterator{}
}

// The following methods implement the Iterator interface.

func (it *pmemIterator) Stats() IteratorStats {
	return IteratorStats{
		TimeBoundNumSSTs:           0,
		InternalDeleteSkippedCount: 0,
	}
}

func (it *pmemIterator) Close() {
	it.destroy()
}

func (it *pmemIterator) Seek(key MVCCKey) {
	it.checkEngineOpen()
	if len(key.Key) == 0 {
		// start=Key("") needs special treatment since we need
		// to access start[0] in an explicit seek.
		it.setState(C.PmemIterSeekToFirst(it.iter))
	} else {
		// We can avoid seeking if we're already at the key we seek.
		if it.valid && !it.reseek && key.Equal(it.UnsafeKey()) {
			return
		}
		it.setState(C.PmemIterSeek(it.iter, goToCPmemKey(key)))
	}
}

func (it *pmemIterator) SeekReverse(key MVCCKey) {
	it.checkEngineOpen()
	if len(key.Key) == 0 {
		it.setState(C.PmemIterSeekToLast(it.iter))
	} else {
		// We can avoid seeking if we're already at the key we seek.
		if it.valid && !it.reseek && key.Equal(it.UnsafeKey()) {
			return
		}
		it.setState(C.PmemIterSeek(it.iter, goToCPmemKey(key)))
		// Maybe the key sorts after the last key in pmem
		if ok, _ := it.Valid(); !ok {
			it.iter(C.PmemIterSeekToLast(it.iter))
		}
		if ok, _ := it.Valid(); !ok {
			return
		}
		// Make sure the current key is <= the provided key.
		if key.Less(it.UnsafeKey()) {
			it.Prev()
		}
	}
}

func (it *pmemIterator) Valid() (bool, error) {
	return it.valid, it.err
}

func (it *pmemIterator) Next() {
	it.checkEngineOpen()
	it.setState(C.PmemIterNext(it.iter, C.bool(false) /* skip_current_key_versions */))
}

func (it *pmemIterator) Prev() {
	it.checkEngineOpen()
	it.setState(C.PmemIterPrev(it.iter, C.bool(false) /* skip_current_key_versions */))
}

func (it *pmemIterator) NextKey() {
	it.checkEngineOpen()
	it.setState(C.PmemIterNext(it.iter, C.bool(true) /* skip_current_key_versions */))
}

func (it *pmemIterator) PrevKey() {
	it.checkEngineOpen()
	it.setState(C.PmemIterPrev(it.iter, C.bool(true) /* skip_current_key_versions */))
}

func (it *pmemIterator) Key() MVCCKey {
	// The data returned by pmem_iter_{key,value} is not meant to be
	// freed by the client. It is a direct reference to the data managed
	// by the iterator, so it is copied instead of freed.
	return cPmemToGoKey(it.key)
}

func (it *pmemIterator) Value() []byte {
	return cPmemSliceToGoBytes(it.value)
}

func (it *pmemIterator) ValueProto(msg protoutil.Message) error {
	if pmem.value.len <= 0 {
		return nil
	}
	return protoutil.Unmarshal(it.UnsafeValue(), msg)
}

func (it *pmemIterator) UnsafeKey() MVCCKey {
	return cPmemToUnsafeGoKey(it.key)
}

func (it *pmemIterator) UnsafeValue() []byte {
	return cPmemSliceToUnsafeGoBytes(it.value)
}

func (it *pmemIterator) clearState() {
	it.valid = false
	it.reseek = true
	it.key = C.PmemKey{}
	it.value = C.PmemSlice{}
	it.err = nil
}

func (it *pmemIterator) setState(state C.PmemIterState) {
	it.valid = bool(state.valid)
	it.reseek = false
	it.key = state.key
	it.value = state.value
	it.err = pmemStatusToError(state.status)
}

func (it *pmemIterator) ComputeStats(
	start, end MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	it.clearState()
	result := C.PmemMVCCComputeStats(it.iter, goToCPmemKey(start), goToCPmemKey(end), C.int64_t(nowNanos))
	stats, err := cPmemStatsToGoStats(result, nowNanos)
	if util.RaceEnabled {
		// If we've come here via batchIterator, then flushMutations (which forces
		// reseek) was called just before C.PmemMVCCComputeStats. Set it here as well
		// to match.
		it.reseek = true
		// C.PmemMVCCComputeStats and ComputeStatsGo must behave identically.
		// There are unit tests to ensure that they return the same result, but
		// as an additional check, use the race builds to check any edge cases
		// that the tests may miss.
		verifyStats, verifyErr := ComputeStatsGo(it, start, end, nowNanos)
		if (err != nil) != (verifyErr != nil) {
			panic(fmt.Sprintf("C.PmemMVCCComputeStats differed from ComputeStatsGo: err %v vs %v", err, verifyErr))
		}
		if !stats.Equal(verifyStats) {
			panic(fmt.Sprintf("C.PmemMVCCComputeStats differed from ComputeStatsGo: stats %+v vs %+v", stats, verifyStats))
		}
	}
	return stats, err
}

func (it *pmemIterator) FindSplitKey(
	start, end, minSplitKey MVCCKey, targetSize int64,
) (MVCCKey, error) {
	var splitKey C.PmemString
	it.clearState()
	status := C.PmemMVCCFindSplitKey(it.iter, goToCPmemKey(start), goToCPmemKey(end), goToCPmemKey(minSplitKey),
		C.int64_t(targetSize), &splitKey)
	if err := pmemStatusToError(status); err != nil {
		return MVCCKey{}, err
	}
	return MVCCKey{Key: cPmemStringToGoBytes(splitKey)}, nil
}

func (it *pmemIterator) MVCCGet(
	key roachpb.Key, timestamp hlc.Timestamp, opts MVCCGetOptions,
) (*roachpb.Value, *roachpb.Intent, error) {
	if opts.Inconsistent && opts.Txn != nil {
		return nil, nil, errors.Errorf("cannot allow inconsistent reads within a transaction")
	}
	if len(key) == 0 {
		return nil, nil, emptyKeyError()
	}

	it.clearState()
	state := C.PmemMVCCGet(
		it.iter, goToCPmemSlice(key), goToCPmemTimestamp(timestamp), goToCTxn(opts.Txn),
		C.bool(opts.Inconsistent), C.bool(opts.Tombstones), C.bool(opts.IgnoreSequence),
	)

	if err := pmemStatusToError(state.status); err != nil {
		return nil, nil, err
	}
	if err := pmemUncertaintyToError(timestamp, state.uncertainty_timestamp, opts.Txn); err != nil {
		return nil, nil, err
	}

	intents, err := buildScanIntents(cPmemSliceToGoBytes(state.intents))
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
	repr := copyFromPmemSliceVector(state.data.bufs, state.data.len)
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

func (it *pmemIterator) MVCCScan(
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

	it.clearState()
	state := C.PmemMVCCScan(
		it.iter, goToCPmemSlice(start), goToCPmemSlice(end),
		goToCPmemTimestamp(timestamp), C.int64_t(max),
		goToCTxn(opts.Txn), C.bool(opts.Inconsistent),
		C.bool(opts.Reverse), C.bool(opts.Tombstones),
		C.bool(opts.IgnoreSequence),
	)

	if err := pmemStatusToError(state.status); err != nil {
		return nil, 0, nil, nil, err
	}
	if err := pmemUncertaintyToError(timestamp, state.uncertainty_timestamp, opts.Txn); err != nil {
		return nil, 0, nil, nil, err
	}

	kvData = copyFromPmemSliceVector(state.data.bufs, state.data.len)
	numKVs = int64(state.data.count)

	if resumeKey := cPmemSliceToGoBytes(state.resume_key); resumeKey != nil {
		if opts.Reverse {
			resumeSpan = &roachpb.Span{Key: start, EndKey: roachpb.Key(resumeKey).Next()}
		} else {
			resumeSpan = &roachpb.Span{Key: resumeKey, EndKey: end}
		}
	}

	intents, err = buildScanIntents(cPmemSliceToGoBytes(state.intents))
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

func (it *pmemIterator) SetUpperBound(key roachpb.Key) {
	C.PmemIterSetUpperBound(it.iter, goToCPmemKey(MakeMVCCMetadataKey(key)))
}

func copyFromPmemSliceVector(bufs *C.PmemSlice, len C.int32_t) []byte {
	if bufs == nil {
		return nil
	}

	// Interpret the C pointer as a pointer to a Go array, then slice.
	slices := (*[1 << 20]C.PmemSlice)(unsafe.Pointer(bufs))[:len:len]
	neededBytes := 0
	for i := range slices {
		neededBytes += int(slices[i].len)
	}
	data := nonZeroingMakeByteSlice(neededBytes)[:0]
	for i := range slices {
		data = append(data, cPmemSliceToUnsafeGoBytes(slices[i])...)
	}
	return data
}

func cPmemStatsToGoStats(stats C.MVCCStatsResult, nowNanos int64) (enginepb.MVCCStats, error) {
	ms := enginepb.MVCCStats{}
	if err := pmemStatusToError(stats.status); err != nil {
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

// goToCPmemSlice converts a go byte slice to a DBSlice. Note that this is
// potentially dangerous as the DBSlice holds a reference to the go
// byte slice memory that the Go GC does not know about. This method
// is only intended for use in converting arguments to C
// functions. The C function must copy any data that it wishes to
// retain once the function returns.
func goToCPmemSlice(b []byte) C.PmemSlice {
	if len(b) == 0 {
		return C.PmemSlice{data: nil, len: 0}
	}
	return C.PmemSlice{
		data: (*C.char)(unsafe.Pointer(&b[0])),
		len:  C.int(len(b)),
	}
}

func goToCPmemKey(key MVCCKey) C.PmemKey {
	return C.PmemKey{
		key:       goToCPmemSlice(key.Key),
		wall_time: C.int64_t(key.Timestamp.WallTime),
		logical:   C.int32_t(key.Timestamp.Logical),
	}
}

func cPmemToGoKey(key C.PmemKey) MVCCKey {
	// When converting a C.PmemKey to an MVCCKey, give the underlying slice an
	// extra byte of capacity in anticipation of roachpb.Key.Next() being
	// called. The extra byte is trivial extra space, but allows callers to avoid
	// an allocation and copy when calling roachpb.Key.Next(). Note that it is
	// important that the extra byte contain the value 0 in order for the
	// roachpb.Key.Next() fast-path to be invoked. This is true for the code
	// below because make() zero initializes all of the bytes.
	unsafeKey := cPmemSliceToUnsafeGoBytes(key.key)
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

func cPmemToUnsafeGoKey(key C.PmemKey) MVCCKey {
	return MVCCKey{
		Key: cPmemSliceToUnsafeGoBytes(key.key),
		Timestamp: hlc.Timestamp{
			WallTime: int64(key.wall_time),
			Logical:  int32(key.logical),
		},
	}
}

func cPmemStringToGoString(s C.PmemString) string {
	if s.data == nil {
		return ""
	}
	result := C.GoStringN(s.data, s.len)
	C.free(unsafe.Pointer(s.data))
	return result
}

func cPmemStringToGoBytes(s C.PmemString) []byte {
	if s.data == nil {
		return nil
	}
	result := gobytes(unsafe.Pointer(s.data), int(s.len))
	C.free(unsafe.Pointer(s.data))
	return result
}

func cPmemSliceToGoBytes(s C.PmemSlice) []byte {
	if s.data == nil {
		return nil
	}
	return gobytes(unsafe.Pointer(s.data), int(s.len))
}

func cPmemSliceToUnsafeGoBytes(s C.PmemSlice) []byte {
	if s.data == nil {
		return nil
	}
	// Interpret the C pointer as a pointer to a Go array, then slice.
	return (*[maxArrayLen]byte)(unsafe.Pointer(s.data))[:s.len:s.len]
}

func goToCPmemTimestamp(ts hlc.Timestamp) C.PmemTimestamp {
	return C.PmemTimestamp{
		wall_time: C.int64_t(ts.WallTime),
		logical:   C.int32_t(ts.Logical),
	}
}

func goToCPmemTxn(txn *roachpb.Transaction) C.PmemTxn {
	var r C.PmemTxn
	if txn != nil {
		r.id = goToCPmemSlice(txn.ID.GetBytes())
		r.epoch = C.uint32_t(txn.Epoch)
		r.sequence = C.int32_t(txn.Sequence)
		r.max_timestamp = goToCPmemTimestamp(txn.MaxTimestamp)
	}
	return r
}

func goToCPmemIterOptions(opts IterOptions) C.PmemIterOptions {
	return C.PmemIterOptions{
		prefix:             C.bool(opts.Prefix),
		lower_bound:        goToCPmemKey(MakeMVCCMetadataKey(opts.LowerBound)),
		upper_bound:        goToCPmemKey(MakeMVCCMetadataKey(opts.UpperBound)),
		min_timestamp_hint: goToCPmemTimestamp(opts.MinTimestampHint),
		max_timestamp_hint: goToCPmemTimestamp(opts.MaxTimestampHint),
		with_stats:         C.bool(opts.WithStats),
	}
}

func pmemStatusToError(s C.PmemStatus) error {
	if s.data == nil {
		return nil
	}
	return &RocksDBError{msg: cPmemStringToGoString(s)}
}

func pmemUncertaintyToError(
	readTS hlc.Timestamp, existingTS C.PmemTimestamp, txn *roachpb.Transaction,
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

// goPmemMerge takes existing and update byte slices that are expected to
// be marshaled roachpb.Values and merges the two values returning a
// marshaled roachpb.Value or an error.
func goPmemMerge(existing, update []byte) ([]byte, error) {
	var result C.PmemString
	status := C.PmemMergeOne(goToCPmemSlice(existing), goToCPmemSlice(update), &result)
	if status.data != nil {
		return nil, errors.Errorf("%s: existing=%q, update=%q",
			cPmemStringToGoString(status), existing, update)
	}
	return cPmemStringToGoBytes(result), nil
}

// goPmemPartialMerge takes existing and update byte slices that are expected to
// be marshaled roachpb.Values and performs a partial merge using C++ code,
// marshaled roachpb.Value or an error.
func goPmemPartialMerge(existing, update []byte) ([]byte, error) {
	var result C.PmemString
	status := C.PmemPartialMergeOne(goToCPmemSlice(existing), goToCPmemSlice(update), &result)
	if status.data != nil {
		return nil, errors.Errorf("%s: existing=%q, update=%q",
			cPmemStringToGoString(status), existing, update)
	}
	return cPmemStringToGoBytes(result), nil
}

func pmemPut(rdb *C.PmemEngine, key MVCCKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	// *Put, *Get, and *Delete call memcpy() (by way of MemTable::Add)
	// when called, so we do not need to worry about these byte slices
	// being reclaimed by the GC.
	return pmemStatusToError(C.PmemPut(rdb, goToCPmemKey(key), goToCPmemSlice(value)))
}

func pmemMerge(rdb *C.PmemEngine, key MVCCKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	// DBMerge calls memcpy() (by way of MemTable::Add)
	// when called, so we do not need to worry about these byte slices being
	// reclaimed by the GC.
	return pmemStatusToError(C.PmemMerge(rdb, goToCPmemKey(key), goToCPmemSlice(value)))
}

func pmemApplyBatchRepr(rdb *C.PmemEngine, repr []byte, sync bool) error {
	return pmemStatusToError(C.PmemApplyBatchRepr(rdb, goToCPmemSlice(repr), C.bool(sync)))
}

// dbGet returns the value for the given key.
func pmemGet(rdb *C.PmemEngine, key MVCCKey) ([]byte, error) {
	if len(key.Key) == 0 {
		return nil, emptyKeyError()
	}
	var result C.PmemString
	err := pmemStatusToError(C.PmemGet(rdb, goToCPmemKey(key), &result))
	if err != nil {
		return nil, err
	}
	return cPmemStringToGoBytes(result), nil
}

func pmemGetProto(
	rdb *C.PmemEngine, key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	if len(key.Key) == 0 {
		err = emptyKeyError()
		return
	}
	var result C.PmemString
	if err = pmemStatusToError(C.PmemGet(rdb, goToCPmemKey(key), &result)); err != nil {
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
		data := cPmemSliceToUnsafeGoBytes(C.PmemSlice(result))
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
	return pmemStatusToError(C.PmemDelete(rdb, goToCPmemKey(key)))
}

func pmemSingleClear(rdb *C.PmemEngine, key MVCCKey) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return pmemStatusToError(C.PmemSingleDelete(rdb, goToCPmemKey(key)))
}

func pmemClearRange(rdb *C.PmemEngine, start, end MVCCKey) error {
	if err := pmemStatusToError(C.PmemDeleteRange(rdb, goToCPmemKey(start), goToCPmemKey(end))); err != nil {
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
	return pmemStatusToError(C.PmemDeleteIterRange(rdb, getter.getIter(), goToCPmemKey(start), goToCPmemKey(end)))
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

func (pmem *PersistentMemoryEngine) IngestExternalFiles(ctx context.Context, paths []string, skipWritingSeqNo, allowFileModifications bool) error {
	// nop
	return nil
}

func (pmem *PersistentMemoryEngine) PreIngestDelay(ctx context.Context) {
	// nop
}

	func (pmem *PersistentMemoryEngine) OpenFile(filename string) (DBFile, error) {
	panic("not supported in PoC")
}

func (pmem *PersistentMemoryEngine) ReadFile(filename string) ([]byte, error) {
	panic("not supported in PoC")
}

func (pmem *PersistentMemoryEngine) DeleteFile(filename string) error {
	panic("not supported in PoC")
}

func (pmem *PersistentMemoryEngine) DeleteDirAndFiles(dir string) error {
	panic("not supported in PoC")
}

func (pmem *PersistentMemoryEngine) LinkFile(oldname, newname string) error {
	panic("not supported in PoC")
}

func (pmem *PersistentMemoryEngine) NewReadOnly() ReadWriter {
	// TODO(jeb) just gonna return 'this' hope it fakes the funk just enough ...
	return pmem
}

func (pmem *PersistentMemoryEngine) NewSnapshot() Reader {
	// TODO(jeb) just gonna return 'this' hope it fakes the funk just enough ...
	return pmem
}

// NewSortedDiskMap implements the MapProvidingEngine interface.
func (pmem *PersistentMemoryEngine) NewSortedDiskMap() diskmap.SortedDiskMap {
	return NewPmemMap(pmem)
}

// NewSortedDiskMultiMap implements the MapProvidingEngine interface.
func (pmem *PersistentMemoryEngine) NewSortedDiskMultiMap() diskmap.SortedDiskMap {
	return NewPmemMultiMap(pmem)
}

// IsValidSplitKey returns whether the key is a valid split key. Certain key
// ranges cannot be split (the meta1 span and the system DB span); split keys
// chosen within any of these ranges are considered invalid. And a split key
// equal to Meta2KeyMax (\x03\xff\xff) is considered invalid.
func IsValidSplitKey(key roachpb.Key) bool {
	return bool(C.MVCCIsValidSplitKey(goToCPmemSlice(key)))
}
