// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the License for the specific language governing
// permissions and limitations under the License.

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// A DBSlice contains read-only data that does not need to be freed.
typedef struct {
  char* data;
  int len;
} DBSlice;

// A DBString is structurally identical to a DBSlice, but the data it
// contains must be freed via a call to free().
typedef struct {
  char* data;
  int len;
} DBString;

// A DBStatus is an alias for DBString and is used to indicate that
// the return value indicates the success or failure of an
// operation. If DBStatus.data == NULL the operation succeeded.
typedef DBString DBStatus;

typedef struct {
  DBSlice key;
  int64_t wall_time;
  int32_t logical;
} DBKey;

typedef struct {
  int64_t wall_time;
  int32_t logical;
} DBTimestamp;

typedef struct {
  bool prefix;
  DBKey lower_bound;
  DBKey upper_bound;
  bool with_stats;
  DBTimestamp min_timestamp_hint;
  DBTimestamp max_timestamp_hint;
} DBIterOptions;

typedef struct {
  bool valid;
  DBKey key;
  DBSlice value;
  DBStatus status;
} DBIterState;

typedef struct DBCache DBCache;
typedef struct PmemEngine PmemEngine;
typedef struct DBIterator DBIterator;
typedef void* DBWritableFile;

// DBOptions contains local database options.
typedef struct {
  int num_cpu;
  bool must_exist;
  bool read_only;
  DBSlice pmem_options;
  DBSlice extra_options;
} PmemOptions;

// Opens the database located in "dir", creating it if it doesn't
// exist.
DBStatus PmemOpen(PmemEngine** db, DBSlice dir, DBOptions options);

// // Destroys the database located in "dir". As the name implies, this
// // operation is destructive. Use with caution.
// DBStatus DBDestroy(DBSlice dir);

// Closes the database, freeing memory and other resources.
DBStatus PmemClose(PmemEngine* db);

// Stores the approximate on-disk size of the given key range into the
// supplied uint64.
DBStatus PmemApproximateDiskBytes(PmemEngine* db, DBKey start, DBKey end, uint64_t* size);

// Sets the database entry for "key" to "value".
DBStatus PmemPut(PmemEngine* db, DBKey key, DBSlice value);

// Merge the database entry (if any) for "key" with "value".
DBStatus PmemMerge(PmemEngine* db, DBKey key, DBSlice value);

// Retrieves the database entry for "key".
DBStatus PmemGet(PmemEngine* db, DBKey key, DBString* value);

// Deletes the database entry for "key".
DBStatus PmemDelete(PmemEngine* db, DBKey key);

// Deletes the most recent database entry for "key". See the following
// documentation for details on the subtleties of this operation:
// https://github.com/facebook/rocksdb/wiki/Single-Delete.
DBStatus PmemSingleDelete(PmemEngine* db, DBKey key);

// Deletes a range of keys from start (inclusive) to end (exclusive).
DBStatus PmemDeleteRange(PmemEngine* db, DBKey start, DBKey end);

// Deletes a range of keys from start (inclusive) to end
// (exclusive). Unlike DBDeleteRange, this function finds the keys to
// delete by iterating over the supplied iterator and creating
// tombstones for the individual keys.
DBStatus PmemDeleteIterRange(PmemEngine* db, DBIterator* iter, DBKey start, DBKey end);

// Applies a batch of operations (puts, merges and deletes) to the
// database atomically and closes the batch. It is only valid to call
// this function on an engine created by DBNewBatch. If an error is
// returned, the batch is not closed and it is the caller's
// responsibility to call DBClose.
DBStatus PmemCommitAndCloseBatch(PmemEngine* db, bool sync);

// ApplyBatchRepr applies a batch of mutations encoded using that
// batch representation returned by DBBatchRepr(). It is only valid to
// call this function on an engine created by DBOpen() or DBNewBatch()
// (i.e. not a snapshot).
DBStatus PmemApplyBatchRepr(PmemEngine* db, DBSlice repr, bool sync);

// Returns the internal batch representation. The returned value is
// only valid until the next call to a method using the PmemEngine and
// should thus be copied immediately. It is only valid to call this
// function on an engine created by DBNewBatch.
DBSlice PmemBatchRepr(PmemEngine* db);

// Creates a new batch for performing a series of operations
// atomically. Use DBCommitBatch() on the returned engine to apply the
// batch to the database. The writeOnly parameter controls whether the
// batch can be used for reads or only for writes. A writeOnly batch
// does not need to index keys for reading and can be faster if the
// number of keys is large (and reads are not necessary). It is the
// caller's responsibility to call DBClose().
PmemEngine* DBNewBatch(PmemEngine* db, bool writeOnly);

// Creates a new database iterator.
//
// When iter_options.prefix is true, Seek will use the user-key prefix of the
// key supplied to DBIterSeek() to restrict which sstables are searched, but
// iteration (using Next) over keys without the same user-key prefix will not
// work correctly (keys may be skipped).
//
// When iter_options.upper_bound is non-nil, the iterator will become invalid
// after seeking past the provided upper bound. This can drastically improve
// performance when seeking within a region covered by range deletion
// tombstones. See #24029 for discussion.
//
// When iter_options.with_stats is true, the iterator will collect RocksDB
// performance counters which can be retrieved via `DBIterStats`.
//
// It is the caller's responsibility to call DBIterDestroy().
DBIterator* DBNewIter(PmemEngine* db, DBIterOptions iter_options);

// Destroys an iterator, freeing up any associated memory.
void DBIterDestroy(DBIterator* iter);

// Positions the iterator at the first key that is >= "key".
DBIterState DBIterSeek(DBIterator* iter, DBKey key);

typedef struct {
  uint64_t internal_delete_skipped_count;
  // the number of SSTables touched (only for time bound iterators).
  // This field is populated from the table filter, not from the
  // RocksDB perf counters.
  //
  // TODO(tschottdorf): populate this field for all iterators.
  uint64_t timebound_num_ssts;
  // New fields added here must also be added in various other places;
  // just grep the repo for internal_delete_skipped_count. Sorry.
} IteratorStats;

IteratorStats DBIterStats(DBIterator* iter);

// Positions the iterator at the first key in the database.
DBIterState DBIterSeekToFirst(DBIterator* iter);

// Positions the iterator at the last key in the database.
DBIterState DBIterSeekToLast(DBIterator* iter);

// Advances the iterator to the next key. If skip_current_key_versions
// is true, any remaining versions for the current key are
// skipped. After this call, DBIterValid() returns 1 iff the iterator
// was not positioned at the last key.
DBIterState DBIterNext(DBIterator* iter, bool skip_current_key_versions);

// Moves the iterator back to the previous key. If
// skip_current_key_versions is true, any remaining versions for the
// current key are skipped. After this call, DBIterValid() returns 1
// iff the iterator was not positioned at the first key.
DBIterState DBIterPrev(DBIterator* iter, bool skip_current_key_versions);

// DBIterSetLowerBound updates this iterator's lower bound.
void DBIterSetLowerBound(DBIterator* iter, DBKey key);

// DBIterSetUpperBound updates this iterator's upper bound.
void DBIterSetUpperBound(DBIterator* iter, DBKey key);

// Implements the merge operator on a single pair of values. update is
// merged with existing. This method is provided for invocation from
// Go code.
DBStatus DBMergeOne(DBSlice existing, DBSlice update, DBString* new_value);

// Implements the partial merge operator on a single pair of values. update is
// merged with existing. This method is provided for invocation from Go code.
DBStatus DBPartialMergeOne(DBSlice existing, DBSlice update, DBString* new_value);

// NB: The function (cStatsToGoStats) that converts these to the go
// representation is unfortunately duplicated in engine and engineccl. If this
// struct is changed, both places need to be updated.
typedef struct {
  DBStatus status;
  int64_t live_bytes;
  int64_t key_bytes;
  int64_t val_bytes;
  int64_t intent_bytes;
  int64_t live_count;
  int64_t key_count;
  int64_t val_count;
  int64_t intent_count;
  int64_t intent_age;
  int64_t gc_bytes_age;
  int64_t sys_bytes;
  int64_t sys_count;
  int64_t last_update_nanos;
} MVCCStatsResult;

MVCCStatsResult MVCCComputeStats(DBIterator* iter, DBKey start, DBKey end, int64_t now_nanos);

bool MVCCIsValidSplitKey(DBSlice key);
DBStatus MVCCFindSplitKey(DBIterator* iter, DBKey start, DBKey end, DBKey min_split,
                          int64_t target_size, DBString* split_key);

// DBTxn contains the fields from a roachpb.Transaction that are
// necessary for MVCC Get and Scan operations. Note that passing a
// serialized roachpb.Transaction appears to be a non-starter as an
// alternative due to the performance overhead.
//
// TODO(peter): We could investigate using
// https://github.com/petermattis/cppgo to generate C++ code that can
// read the Go roachpb.Transaction structure.
typedef struct {
  DBSlice id;
  uint32_t epoch;
  int32_t sequence;
  DBTimestamp max_timestamp;
} DBTxn;

typedef struct {
  DBSlice* bufs;
  // len is the number of DBSlices in bufs.
  int32_t len;
  // count is the number of key/value pairs in bufs.
  int32_t count;
} DBChunkedBuffer;

// DBScanResults contains the key/value pairs and intents encoded
// using the RocksDB batch repr format.
typedef struct {
  DBStatus status;
  DBChunkedBuffer data;
  DBSlice intents;
  DBTimestamp uncertainty_timestamp;
  DBSlice resume_key;
} DBScanResults;

DBScanResults MVCCGet(DBIterator* iter, DBSlice key, DBTimestamp timestamp, DBTxn txn,
                      bool inconsistent, bool tombstones, bool ignore_sequence);
DBScanResults MVCCScan(DBIterator* iter, DBSlice start, DBSlice end, DBTimestamp timestamp,
                       int64_t max_keys, DBTxn txn, bool inconsistent, bool reverse,
                       bool tombstones, bool ignore_sequence);

#ifdef __cplusplus
}  // extern "C"
#endif
