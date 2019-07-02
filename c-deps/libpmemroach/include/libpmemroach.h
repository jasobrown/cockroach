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

// A PmemSlice contains read-only data that does not need to be freed.
typedef struct {
  char* data;
  int len;
} PmemSlice;

// A PmemString is structurally identical to a PmemSlice, but the data it
// contains must be freed via a call to free().
typedef struct {
  char* data;
  int len;
} PmemString;

// A PmemStatus is an alias for PmemString and is used to indicate that
// the return value indicates the success or failure of an
// operation. If PmemStatus.data == NULL the operation succeeded.
typedef PmemString PmemStatus;

typedef struct {
  PmemSlice key;
  int64_t wall_time;
  int32_t logical;
} PmemKey;

typedef struct {
  int64_t wall_time;
  int32_t logical;
} PmemTimestamp;

typedef struct {
  bool prefix;
  PmemKey lower_bound;
  PmemKey upper_bound;
  bool with_stats;
  PmemTimestamp min_timestamp_hint;
  PmemTimestamp max_timestamp_hint;
} PmemIterOptions;

typedef struct {
  bool valid;
  PmemKey key;
  PmemSlice value;
  PmemStatus status;
} PmemIterState;

typedef struct PmemEngine PmemEngine;
typedef struct PmemIterator PmemIterator;

// PmemOptions contains local database options.
typedef struct {
  int num_cpu;
  bool must_exist;
  bool read_only;
  PmemSlice pmem_options;
  PmemSlice extra_options;
} PmemOptions;

// Opens the database located in "dir", creating it if it doesn't
// exist.
PmemStatus PmemOpen(PmemEngine** db, PmemSlice dir, PmemOptions options);

// // Destroys the database located in "dir". As the name implies, this
// // operation is destructive. Use with caution.
// PmemStatus PmemDestroy(PmemSlice dir);

// Closes the database, freeing memory and other resources.
PmemStatus PmemClose(PmemEngine* db);

// Stores the approximate on-disk size of the given key range into the
// supplied uint64.
PmemStatus PmemApproximateDiskBytes(PmemEngine* db, PmemKey start, PmemKey end, uint64_t* size);

// Sets the database entry for "key" to "value".
PmemStatus PmemPut(PmemEngine* db, PmemKey key, PmemSlice value);

// Merge the database entry (if any) for "key" with "value".
PmemStatus PmemMerge(PmemEngine* db, PmemKey key, PmemSlice value);

// Retrieves the database entry for "key".
PmemStatus PmemGet(PmemEngine* db, PmemKey key, PmemString* value);

// Deletes the database entry for "key".
PmemStatus PmemDelete(PmemEngine* db, PmemKey key);

// Deletes the most recent database entry for "key". See the following
// documentation for details on the subtleties of this operation:
// https://github.com/facebook/rocksdb/wiki/Single-Delete.
PmemStatus PmemSingleDelete(PmemEngine* db, PmemKey key);

// Deletes a range of keys from start (inclusive) to end (exclusive).
PmemStatus PmemDeleteRange(PmemEngine* db, PmemKey start, PmemKey end);

// Deletes a range of keys from start (inclusive) to end
// (exclusive). Unlike PmemDeleteRange, this function finds the keys to
// delete by iterating over the supplied iterator and creating
// tombstones for the individual keys.
PmemStatus PmemDeleteIterRange(PmemEngine* db, PmemIterator* iter, PmemKey start, PmemKey end);

// Applies a batch of operations (puts, merges and deletes) to the
// database atomically and closes the batch. It is only valid to call
// this function on an engine created by PmemNewBatch. If an error is
// returned, the batch is not closed and it is the caller's
// responsibility to call PmemClose.
PmemStatus PmemCommitAndCloseBatch(PmemEngine* db, bool sync);

// ApplyBatchRepr applies a batch of mutations encoded using that
// batch representation returned by PmemBatchRepr(). It is only valid to
// call this function on an engine created by PmemOpen() or PmemNewBatch()
// (i.e. not a snapshot).
PmemStatus PmemApplyBatchRepr(PmemEngine* db, PmemSlice repr, bool sync);

// Returns the internal batch representation. The returned value is
// only valid until the next call to a method using the PmemEngine and
// should thus be copied immediately. It is only valid to call this
// function on an engine created by PmemNewBatch.
PmemSlice PmemBatchRepr(PmemEngine* db);

// Creates a new batch for performing a series of operations
// atomically. Use PmemCommitBatch() on the returned engine to apply the
// batch to the database. The writeOnly parameter controls whether the
// batch can be used for reads or only for writes. A writeOnly batch
// does not need to index keys for reading and can be faster if the
// number of keys is large (and reads are not necessary). It is the
// caller's responsibility to call PmemClose().
    //PmemEngine* PmemNewBatch(PmemEngine* db, bool writeOnly);

// Creates a new database iterator.
//
// When iter_options.prefix is true, Seek will use the user-key prefix of the
// key supplied to PmemIterSeek() to restrict which sstables are searched, but
// iteration (using Next) over keys without the same user-key prefix will not
// work correctly (keys may be skipped).
//
// When iter_options.upper_bound is non-nil, the iterator will become invalid
// after seeking past the provided upper bound. This can drastically improve
// performance when seeking within a region covered by range deletion
// tombstones. See #24029 for discussion.
//
// It is the caller's responsibility to call PmemIterDestroy().
PmemIterator* PmemNewIter(PmemEngine* db, PmemIterOptions iter_options);

// Destroys an iterator, freeing up any associated memory.
void PmemIterDestroy(PmemIterator* iter);

// Positions the iterator at the first key that is >= "key".
PmemIterState PmemIterSeek(PmemIterator* iter, PmemKey key);

// Positions the iterator at the first key in the database.
PmemIterState PmemIterSeekToFirst(PmemIterator* iter);

// Positions the iterator at the last key in the database.
PmemIterState PmemIterSeekToLast(PmemIterator* iter);

// Advances the iterator to the next key. If skip_current_key_versions
// is true, any remaining versions for the current key are
// skipped. After this call, PmemIterValid() returns 1 iff the iterator
// was not positioned at the last key.
PmemIterState PmemIterNext(PmemIterator* iter, bool skip_current_key_versions);

// Moves the iterator back to the previous key. If
// skip_current_key_versions is true, any remaining versions for the
// current key are skipped. After this call, PmemIterValid() returns 1
// iff the iterator was not positioned at the first key.
PmemIterState PmemIterPrev(PmemIterator* iter, bool skip_current_key_versions);

// PmemIterSetLowerBound updates this iterator's lower bound.
void PmemIterSetLowerBound(PmemIterator* iter, PmemKey key);

// PmemIterSetUpperBound updates this iterator's upper bound.
void PmemIterSetUpperBound(PmemIterator* iter, PmemKey key);

// Implements the merge operator on a single pair of values. update is
// merged with existing. This method is provided for invocation from
// Go code.
PmemStatus PmemMergeOne(PmemSlice existing, PmemSlice update, PmemString* new_value);

// Implements the partial merge operator on a single pair of values. update is
// merged with existing. This method is provided for invocation from Go code.
PmemStatus PmemPartialMergeOne(PmemSlice existing, PmemSlice update, PmemString* new_value);

// NB: The function (cStatsToGoStats) that converts these to the go
// representation is unfortunately duplicated in engine and engineccl. If this
// struct is changed, both places need to be updated.
typedef struct {
  PmemStatus status;
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
} PmemMVCCStatsResult;

PmemMVCCStatsResult PmemMVCCComputeStats(PmemIterator* iter, PmemKey start, PmemKey end, int64_t now_nanos);

bool PmemMVCCIsValidSplitKey(PmemSlice key);
PmemStatus PmemMVCCFindSplitKey(PmemIterator* iter, PmemKey start, PmemKey end, PmemKey min_split,
                          int64_t target_size, PmemString* split_key);

// PmemTxn contains the fields from a roachpb.Transaction that are
// necessary for MVCC Get and Scan operations. Note that passing a
// serialized roachpb.Transaction appears to be a non-starter as an
// alternative due to the performance overhead.
//
// TODO(peter): We could investigate using
// https://github.com/petermattis/cppgo to generate C++ code that can
// read the Go roachpb.Transaction structure.
typedef struct {
  PmemSlice id;
  uint32_t epoch;
  int32_t sequence;
  PmemTimestamp max_timestamp;
} PmemTxn;

typedef struct {
  PmemSlice* bufs;
  // len is the number of PmemSlices in bufs.
  int32_t len;
  // count is the number of key/value pairs in bufs.
  int32_t count;
} PmemChunkedBuffer;

// PmemScanResults contains the key/value pairs and intents encoded
// using the RocksPmem batch repr format.
typedef struct {
  PmemStatus status;
  PmemChunkedBuffer data;
  PmemSlice intents;
  PmemTimestamp uncertainty_timestamp;
  PmemSlice resume_key;
} PmemScanResults;

PmemScanResults PmemMVCCGet(PmemIterator* iter, PmemSlice key, PmemTimestamp timestamp, PmemTxn txn,
                      bool inconsistent, bool tombstones, bool ignore_sequence);
PmemScanResults PmemMVCCScan(PmemIterator* iter, PmemSlice start, PmemSlice end, PmemTimestamp timestamp,
                       int64_t max_keys, PmemTxn txn, bool inconsistent, bool reverse,
                       bool tombstones, bool ignore_sequence);

#ifdef __cplusplus
}  // extern "C"
#endif
