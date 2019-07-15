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
#include "defines.h"
#include "encoding.h"
#include "engine.h"
#include "fmt.h"
#include "godefs.h"
#include "iterator.h"
#include "merge.h"
#include "options.h"
#include "status.h"

using namespace cockroach;

namespace {

PmemIterState PmemIterGetState(PmemIterator* iter) {
  PmemIterState state = {};
  // state.valid = iter->rep->Valid();
  // state.status = ToPmemStatus(iter->rep->status());

  // if (state.valid) {
  //   rocksdb::Slice key;
  //   state.valid = DecodeKey(iter->rep->key(), &key, &state.key.wall_time, &state.key.logical);
  //   if (state.valid) {
  //     state.key.key = ToPmemSlice(key);
  //     state.value = ToPmemSlice(iter->rep->value());
  //   }
  // }

  return state;
}

}  // namespace

//namespace cockroach {

PmemStatus PmemDestroy(PmemSlice dir) {
  // rocksdb::Options options;
  // return ToPmemStatus(rocksdb::DestroyPmem(ToString(dir), options));
    return kSuccess;
}

PmemStatus PmemClose(PmemEngine* db) {
  PmemStatus status = db->AssertPreClose();
  if (status.data == nullptr) {
    delete db;
  }
  return status;
}

PmemStatus PmemApproximateDiskBytes(PmemEngine* db, PmemKey start, PmemKey end, uint64_t* size) {
  const std::string start_key(EncodeKey(start));
  const std::string end_key(EncodeKey(end));
  // const rocksdb::Range r(start_key, end_key);
  // const uint8_t flags = rocksdb::Pmem::SizeApproximationFlags::INCLUDE_FILES;

  // db->rep->GetApproximateSizes(&r, 1, size, flags);
  return kSuccess;
}

PmemStatus PmemPut(PmemEngine* db, PmemKey key, PmemSlice value) { return db->Put(key, value); }

PmemStatus PmemMerge(PmemEngine* db, PmemKey key, PmemSlice value) { return db->Merge(key, value); }

PmemStatus PmemGet(PmemEngine* db, PmemKey key, PmemString* value) { return db->Get(key, value); }

PmemStatus PmemDelete(PmemEngine* db, PmemKey key) { return db->Delete(key); }

PmemStatus PmemSingleDelete(PmemEngine* db, PmemKey key) { return db->SingleDelete(key); }

PmemStatus PmemDeleteRange(PmemEngine* db, PmemKey start, PmemKey end) { return db->DeleteRange(start, end); }

PmemStatus PmemDeleteIterRange(PmemEngine* db, PmemIterator* iter, PmemKey start, PmemKey end) {
  // rocksdb::Iterator* const iter_rep = iter->rep.get();
  // iter_rep->Seek(EncodeKey(start));
  // const std::string end_key = EncodeKey(end);
  // for (; iter_rep->Valid() && kComparator.Compare(iter_rep->key(), end_key) < 0; iter_rep->Next()) {
  //   PmemStatus status = db->Delete(ToPmemKey(iter_rep->key()));
  //   if (status.data != NULL) {
  //     return status;
  //   }
  // }
  return kSuccess;
}

PmemStatus PmemCommitAndCloseBatch(PmemEngine* db, bool sync) {
  PmemStatus status = db->CommitBatch(sync);
  if (status.data == NULL) {
    PmemClose(db);
  }
  return status;
}

PmemStatus PmemApplyBatchRepr(PmemEngine* db, PmemSlice repr, bool sync) {
  return db->ApplyBatchRepr(repr, sync);
}

PmemSlice PmemBatchRepr(PmemEngine* db) { return db->BatchRepr(); }

// PmemEngine* PmemNewBatch(PmemEngine* db, bool writeOnly) {
//   if (writeOnly) {
//     return new PmemWriteOnlyBatch(db);
//   }
//   return new PmemBatch(db);
// }

PmemIterator* PmemNewIter(PmemEngine* db, PmemIterOptions iter_options) {
  return db->NewIter(iter_options);
}

void PmemIterDestroy(PmemIterator* iter) { delete iter; }

PmemIterState PmemIterSeek(PmemIterator* iter, PmemKey key) {
    //  iter->rep->Seek(EncodeKey(key));
  return PmemIterGetState(iter);
}

PmemIterState PmemIterSeekToFirst(PmemIterator* iter) {
    //  iter->rep->SeekToFirst();
  return PmemIterGetState(iter);
}

PmemIterState PmemIterSeekToLast(PmemIterator* iter) {
    //  iter->rep->SeekToLast();
  return PmemIterGetState(iter);
}

PmemIterState PmemIterNext(PmemIterator* iter, bool skip_current_key_versions) {
    //  ScopedStats stats(iter);
  // If we're skipping the current key versions, remember the key the
  // iterator was pointing out.
  // std::string old_key;
  // if (skip_current_key_versions && iter->rep->Valid()) {
  //   rocksdb::Slice key;
  //   rocksdb::Slice ts;
  //   if (!SplitKey(iter->rep->key(), &key, &ts)) {
  //     PmemIterState state = {0};
  //     state.valid = false;
  //     state.status = FmtStatus("failed to split mvcc key");
  //     return state;
  //   }
  //   old_key = key.ToString();
  // }

  // iter->rep->Next();

  // if (skip_current_key_versions && iter->rep->Valid()) {
  //   rocksdb::Slice key;
  //   rocksdb::Slice ts;
  //   if (!SplitKey(iter->rep->key(), &key, &ts)) {
  //     PmemIterState state = {0};
  //     state.valid = false;
  //     state.status = FmtStatus("failed to split mvcc key");
  //     return state;
  //   }
  //   if (old_key == key) {
  //     // We're pointed at a different version of the same key. Fall
  //     // back to seeking to the next key.
  //     old_key.append("\0", 1);
  //     PmemKey db_key;
  //     db_key.key = ToPmemSlice(old_key);
  //     db_key.wall_time = 0;
  //     db_key.logical = 0;
  //     iter->rep->Seek(EncodeKey(db_key));
  //   }
  // }

  return PmemIterGetState(iter);
}

PmemIterState PmemIterPrev(PmemIterator* iter, bool skip_current_key_versions) {
  // ScopedStats stats(iter);
  // // If we're skipping the current key versions, remember the key the
  // // iterator was pointed out.
  // std::string old_key;
  // if (skip_current_key_versions && iter->rep->Valid()) {
  //   rocksdb::Slice key;
  //   rocksdb::Slice ts;
  //   if (SplitKey(iter->rep->key(), &key, &ts)) {
  //     old_key = key.ToString();
  //   }
  // }

  // iter->rep->Prev();

  // if (skip_current_key_versions && iter->rep->Valid()) {
  //   rocksdb::Slice key;
  //   rocksdb::Slice ts;
  //   if (SplitKey(iter->rep->key(), &key, &ts)) {
  //     if (old_key == key) {
  //       // We're pointed at a different version of the same key. Fall
  //       // back to seeking to the prev key. In this case, we seek to
  //       // the "metadata" key and that back up the iterator.
  //       PmemKey db_key;
  //       db_key.key = ToPmemSlice(old_key);
  //       db_key.wall_time = 0;
  //       db_key.logical = 0;
  //       iter->rep->Seek(EncodeKey(db_key));
  //       if (iter->rep->Valid()) {
  //         iter->rep->Prev();
  //       }
  //     }
  //   }
  // }

  return PmemIterGetState(iter);
}

void PmemIterSetLowerBound(PmemIterator* iter, PmemKey key) { iter->SetLowerBound(key); }
void PmemIterSetUpperBound(PmemIterator* iter, PmemKey key) { iter->SetUpperBound(key); }

PmemStatus PmemMerge(PmemSlice existing, PmemSlice update, PmemString* new_value, bool full_merge) {
  new_value->len = 0;

  cockroach::storage::engine::enginepb::MVCCMetadata meta;
  // if (!meta.ParseFromArray(existing.data, existing.len)) {
  //   return ToPmemString("corrupted existing value");
  // }

  // cockroach::storage::engine::enginepb::MVCCMetadata update_meta;
  // if (!update_meta.ParseFromArray(update.data, update.len)) {
  //   return ToPmemString("corrupted update value");
  // }

  // if (!MergeValues(&meta, update_meta, full_merge, NULL)) {
  //   return ToPmemString("incompatible merge values");
  // }
  return MergeResult(&meta, new_value);
}

PmemStatus PmemMergeOne(PmemSlice existing, PmemSlice update, PmemString* new_value) {
  return PmemMerge(existing, update, new_value, true);
}

PmemStatus PmemPartialMergeOne(PmemSlice existing, PmemSlice update, PmemString* new_value) {
  return PmemMerge(existing, update, new_value, false);
}

