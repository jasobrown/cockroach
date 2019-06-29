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

#include "db.h"
#include <algorithm>
#include <stdarg.h>
#include "batch.h"
#include "comparator.h"
#include "defines.h"
#include "encoding.h"
#include "engine.h"
#include "env_manager.h"
#include "fmt.h"
#include "getter.h"
#include "godefs.h"
#include "iterator.h"
#include "merge.h"
#include "options.h"
#include "snapshot.h"
#include "status.h"
#include "table_props.h"

using namespace cockroach;

namespace cockroach {

DBKey ToDBKey(const rocksdb::Slice& s) {
  DBKey key;
  memset(&key, 0, sizeof(key));
  rocksdb::Slice tmp;
  if (DecodeKey(s, &tmp, &key.wall_time, &key.logical)) {
    key.key = ToDBSlice(tmp);
  }
  return key;
}

ScopedStats::ScopedStats(DBIterator* iter)
    : iter_(iter),
      internal_delete_skipped_count_base_(
          rocksdb::get_perf_context()->internal_delete_skipped_count) {
  if (iter_->stats != nullptr) {
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
  }
}
ScopedStats::~ScopedStats() {
  if (iter_->stats != nullptr) {
    iter_->stats->internal_delete_skipped_count +=
        (rocksdb::get_perf_context()->internal_delete_skipped_count -
         internal_delete_skipped_count_base_);
    rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);
  }
}

void BatchSSTablesForCompaction(const std::vector<rocksdb::SstFileMetaData>& sst,
                                rocksdb::Slice start_key, rocksdb::Slice end_key,
                                uint64_t target_size, std::vector<rocksdb::Range>* ranges) {
  int prev = -1;  // index of the last compacted sst
  uint64_t size = 0;
  for (int i = 0; i < sst.size(); ++i) {
    size += sst[i].size;
    if (size < target_size && (i + 1) < sst.size()) {
      // We haven't reached the target size or the end of the sstables
      // to compact.
      continue;
    }

    rocksdb::Slice start;
    if (prev == -1) {
      // This is the first compaction.
      start = start_key;
    } else {
      // This is a compaction in the middle or end of the requested
      // key range. The start key for the compaction is the largest
      // key from the previous compacted.
      start = rocksdb::Slice(sst[prev].largestkey);
    }

    rocksdb::Slice end;
    if ((i + 1) == sst.size()) {
      // This is the last compaction.
      end = end_key;
    } else {
      // This is a compaction at the start or in the middle of the
      // requested key range. The end key is the largest key in the
      // current sstable.
      end = rocksdb::Slice(sst[i].largestkey);
    }

    ranges->emplace_back(rocksdb::Range(start, end));

    prev = i;
    size = 0;
  }
}

}  // namespace cockroach

namespace {

DBIterState DBIterGetState(DBIterator* iter) {
  DBIterState state = {};
  state.valid = iter->rep->Valid();
  state.status = ToDBStatus(iter->rep->status());

  if (state.valid) {
    rocksdb::Slice key;
    state.valid = DecodeKey(iter->rep->key(), &key, &state.key.wall_time, &state.key.logical);
    if (state.valid) {
      state.key.key = ToDBSlice(key);
      state.value = ToDBSlice(iter->rep->value());
    }
  }

  return state;
}

}  // namespace

namespace cockroach {

// DBOpenHookOSS mode only verifies that no extra options are specified.
rocksdb::Status DBOpenHookOSS(std::shared_ptr<rocksdb::Logger> info_log, const std::string& db_dir,
                              const DBOptions db_opts, EnvManager* env_mgr) {
  if (db_opts.extra_options.len != 0) {
    return rocksdb::Status::InvalidArgument("encryption options are not supported in OSS builds");
  }
  return rocksdb::Status::OK();
}

}  // namespace cockroach

static DBOpenHook* db_open_hook = DBOpenHookOSS;

DBStatus DBDestroy(DBSlice dir) {
  rocksdb::Options options;
  return ToDBStatus(rocksdb::DestroyDB(ToString(dir), options));
}

DBStatus DBClose(PmemEngine* db) {
  DBStatus status = db->AssertPreClose();
  if (status.data == nullptr) {
    delete db;
  }
  return status;
}

DBStatus DBApproximateDiskBytes(PmemEngine* db, DBKey start, DBKey end, uint64_t* size) {
  const std::string start_key(EncodeKey(start));
  const std::string end_key(EncodeKey(end));
  const rocksdb::Range r(start_key, end_key);
  const uint8_t flags = rocksdb::DB::SizeApproximationFlags::INCLUDE_FILES;

  db->rep->GetApproximateSizes(&r, 1, size, flags);
  return kSuccess;
}

DBStatus PmemPut(PmemEngine* db, DBKey key, DBSlice value) { return db->Put(key, value); }

DBStatus PmemMerge(PmemEngine* db, DBKey key, DBSlice value) { return db->Merge(key, value); }

DBStatus PmemGet(PmemEngine* db, DBKey key, DBString* value) { return db->Get(key, value); }

DBStatus PmemDelete(PmemEngine* db, DBKey key) { return db->Delete(key); }

DBStatus PmemSingleDelete(PmemEngine* db, DBKey key) { return db->SingleDelete(key); }

DBStatus PmemDeleteRange(PmemEngine* db, DBKey start, DBKey end) { return db->DeleteRange(start, end); }

DBStatus PmemDeleteIterRange(PmemEngine* db, DBIterator* iter, DBKey start, DBKey end) {
  rocksdb::Iterator* const iter_rep = iter->rep.get();
  iter_rep->Seek(EncodeKey(start));
  const std::string end_key = EncodeKey(end);
  for (; iter_rep->Valid() && kComparator.Compare(iter_rep->key(), end_key) < 0; iter_rep->Next()) {
    DBStatus status = db->Delete(ToDBKey(iter_rep->key()));
    if (status.data != NULL) {
      return status;
    }
  }
  return kSuccess;
}

DBStatus PmemCommitAndCloseBatch(PmemEngine* db, bool sync) {
  DBStatus status = db->CommitBatch(sync);
  if (status.data == NULL) {
    DBClose(db);
  }
  return status;
}

DBStatus PmemApplyBatchRepr(PmemEngine* db, DBSlice repr, bool sync) {
  return db->ApplyBatchRepr(repr, sync);
}

DBSlice PmemBatchRepr(PmemEngine* db) { return db->BatchRepr(); }

PmemEngine* DBNewBatch(PmemEngine* db, bool writeOnly) {
  if (writeOnly) {
    return new DBWriteOnlyBatch(db);
  }
  return new DBBatch(db);
}

DBIterator* DBNewIter(PmemEngine* db, DBIterOptions iter_options) {
  return db->NewIter(iter_options);
}

void DBIterDestroy(DBIterator* iter) { delete iter; }

IteratorStats DBIterStats(DBIterator* iter) {
  IteratorStats stats = {};
  if (iter->stats != nullptr) {
    stats = *iter->stats;
  }
  return stats;
}

DBIterState DBIterSeek(DBIterator* iter, DBKey key) {
  ScopedStats stats(iter);
  iter->rep->Seek(EncodeKey(key));
  return DBIterGetState(iter);
}

DBIterState DBIterSeekToFirst(DBIterator* iter) {
  ScopedStats stats(iter);
  iter->rep->SeekToFirst();
  return DBIterGetState(iter);
}

DBIterState DBIterSeekToLast(DBIterator* iter) {
  ScopedStats stats(iter);
  iter->rep->SeekToLast();
  return DBIterGetState(iter);
}

DBIterState DBIterNext(DBIterator* iter, bool skip_current_key_versions) {
  ScopedStats stats(iter);
  // If we're skipping the current key versions, remember the key the
  // iterator was pointing out.
  std::string old_key;
  if (skip_current_key_versions && iter->rep->Valid()) {
    rocksdb::Slice key;
    rocksdb::Slice ts;
    if (!SplitKey(iter->rep->key(), &key, &ts)) {
      DBIterState state = {0};
      state.valid = false;
      state.status = FmtStatus("failed to split mvcc key");
      return state;
    }
    old_key = key.ToString();
  }

  iter->rep->Next();

  if (skip_current_key_versions && iter->rep->Valid()) {
    rocksdb::Slice key;
    rocksdb::Slice ts;
    if (!SplitKey(iter->rep->key(), &key, &ts)) {
      DBIterState state = {0};
      state.valid = false;
      state.status = FmtStatus("failed to split mvcc key");
      return state;
    }
    if (old_key == key) {
      // We're pointed at a different version of the same key. Fall
      // back to seeking to the next key.
      old_key.append("\0", 1);
      DBKey db_key;
      db_key.key = ToDBSlice(old_key);
      db_key.wall_time = 0;
      db_key.logical = 0;
      iter->rep->Seek(EncodeKey(db_key));
    }
  }

  return DBIterGetState(iter);
}

DBIterState DBIterPrev(DBIterator* iter, bool skip_current_key_versions) {
  ScopedStats stats(iter);
  // If we're skipping the current key versions, remember the key the
  // iterator was pointed out.
  std::string old_key;
  if (skip_current_key_versions && iter->rep->Valid()) {
    rocksdb::Slice key;
    rocksdb::Slice ts;
    if (SplitKey(iter->rep->key(), &key, &ts)) {
      old_key = key.ToString();
    }
  }

  iter->rep->Prev();

  if (skip_current_key_versions && iter->rep->Valid()) {
    rocksdb::Slice key;
    rocksdb::Slice ts;
    if (SplitKey(iter->rep->key(), &key, &ts)) {
      if (old_key == key) {
        // We're pointed at a different version of the same key. Fall
        // back to seeking to the prev key. In this case, we seek to
        // the "metadata" key and that back up the iterator.
        DBKey db_key;
        db_key.key = ToDBSlice(old_key);
        db_key.wall_time = 0;
        db_key.logical = 0;
        iter->rep->Seek(EncodeKey(db_key));
        if (iter->rep->Valid()) {
          iter->rep->Prev();
        }
      }
    }
  }

  return DBIterGetState(iter);
}

void DBIterSetLowerBound(DBIterator* iter, DBKey key) { iter->SetLowerBound(key); }
void DBIterSetUpperBound(DBIterator* iter, DBKey key) { iter->SetUpperBound(key); }

DBStatus DBMerge(DBSlice existing, DBSlice update, DBString* new_value, bool full_merge) {
  new_value->len = 0;

  cockroach::storage::engine::enginepb::MVCCMetadata meta;
  if (!meta.ParseFromArray(existing.data, existing.len)) {
    return ToDBString("corrupted existing value");
  }

  cockroach::storage::engine::enginepb::MVCCMetadata update_meta;
  if (!update_meta.ParseFromArray(update.data, update.len)) {
    return ToDBString("corrupted update value");
  }

  if (!MergeValues(&meta, update_meta, full_merge, NULL)) {
    return ToDBString("incompatible merge values");
  }
  return MergeResult(&meta, new_value);
}

DBStatus DBMergeOne(DBSlice existing, DBSlice update, DBString* new_value) {
  return DBMerge(existing, update, new_value, true);
}

DBStatus DBPartialMergeOne(DBSlice existing, DBSlice update, DBString* new_value) {
  return DBMerge(existing, update, new_value, false);
}

