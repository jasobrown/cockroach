// Copyright 2018 The Cockroach Authors.
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

#include <libroach.h>
#include <rocksdb/db.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include "engine.h"

namespace cockroach {

struct PmemBatch : public PmemEngine {
  int updates;
  bool has_delete_range;
  rocksdb::WriteBatchWithIndex batch;

  PmemBatch(PmemEngine* db);
  virtual ~PmemBatch();

  virtual PmemStatus Put(PmemKey key, PmemSlice value);
  virtual PmemStatus Merge(PmemKey key, PmemSlice value);
  virtual PmemStatus Delete(PmemKey key);
  virtual PmemStatus SingleDelete(PmemKey key);
  virtual PmemStatus DeleteRange(PmemKey start, PmemKey end);
  virtual PmemStatus CommitBatch(bool sync);
  virtual PmemStatus ApplyBatchRepr(PmemSlice repr, bool sync);
  virtual PmemSlice BatchRepr();
  virtual PmemStatus Get(PmemKey key, PmemString* value);
  virtual PmemIterator* NewIter(PmemIterOptions);
  virtual PmemStatus GetStats(PmemStatsResult* stats);
  virtual PmemStatus GetTickersAndHistograms(PmemTickersAndHistogramsResult* stats);
  virtual PmemString GetCompactionStats();
  virtual PmemStatus GetEnvStats(PmemEnvStatsResult* stats);
  virtual PmemStatus GetEncryptionRegistries(PmemEncryptionRegistries* result);
  virtual PmemStatus EnvWriteFile(PmemSlice path, PmemSlice contents);
  virtual PmemStatus EnvOpenFile(PmemSlice path, rocksdb::WritableFile** file);
  virtual PmemStatus EnvReadFile(PmemSlice path, PmemSlice* contents);
  virtual PmemStatus EnvAppendFile(rocksdb::WritableFile* file, PmemSlice contents);
  virtual PmemStatus EnvSyncFile(rocksdb::WritableFile* file);
  virtual PmemStatus EnvCloseFile(rocksdb::WritableFile* file);
  virtual PmemStatus EnvDeleteFile(PmemSlice path);
  virtual PmemStatus EnvDeleteDirAndFiles(PmemSlice dir);
  virtual PmemStatus EnvLinkFile(PmemSlice oldname, PmemSlice newname);
};

struct PmemWriteOnlyBatch : public PmemEngine {
  int updates;
  rocksdb::WriteBatch batch;

  PmemWriteOnlyBatch(PmemEngine* db);
  virtual ~PmemWriteOnlyBatch();

  virtual PmemStatus Put(PmemKey key, PmemSlice value);
  virtual PmemStatus Merge(PmemKey key, PmemSlice value);
  virtual PmemStatus Delete(PmemKey key);
  virtual PmemStatus SingleDelete(PmemKey key);
  virtual PmemStatus DeleteRange(PmemKey start, PmemKey end);
  virtual PmemStatus CommitBatch(bool sync);
  virtual PmemStatus ApplyBatchRepr(PmemSlice repr, bool sync);
  virtual PmemSlice BatchRepr();
  virtual PmemStatus Get(PmemKey key, PmemString* value);
  virtual PmemIterator* NewIter(PmemIterOptions);
  virtual PmemStatus GetStats(PmemStatsResult* stats);
  virtual PmemStatus GetTickersAndHistograms(PmemTickersAndHistogramsResult* stats);
  virtual PmemString GetCompactionStats();
  virtual PmemString GetEnvStats(PmemEnvStatsResult* stats);
  virtual PmemStatus GetEncryptionRegistries(PmemEncryptionRegistries* result);
  virtual PmemStatus EnvWriteFile(PmemSlice path, PmemSlice contents);
  virtual PmemStatus EnvOpenFile(PmemSlice path, rocksdb::WritableFile** file);
  virtual PmemStatus EnvReadFile(PmemSlice path, PmemSlice* contents);
  virtual PmemStatus EnvAppendFile(rocksdb::WritableFile* file, PmemSlice contents);
  virtual PmemStatus EnvSyncFile(rocksdb::WritableFile* file);
  virtual PmemStatus EnvCloseFile(rocksdb::WritableFile* file);
  virtual PmemStatus EnvDeleteFile(PmemSlice path);
  virtual PmemStatus EnvDeleteDirAndFiles(PmemSlice dir);
  virtual PmemStatus EnvLinkFile(PmemSlice oldname, PmemSlice newname);
};

// GetPmemBatchInserter returns a WriteBatch::Handler that operates on a
// WriteBatchBase. The caller assumes ownership of the returned handler.
::rocksdb::WriteBatch::Handler* GetPmemBatchInserter(::rocksdb::WriteBatchBase* batch);

}  // namespace cockroach
