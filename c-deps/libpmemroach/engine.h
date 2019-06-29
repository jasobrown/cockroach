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

#include <atomic>
#include <libroach.h>
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/statistics.h>
#include "eventlistener.h"

struct PmemEngine {
  rocksdb::Pmem* const rep;
  std::atomic<int64_t>* iters;

  PmemEngine(rocksdb::Pmem* r, std::atomic<int64_t>* iters) : rep(r), iters(iters) {}
  virtual ~PmemEngine();

  virtual PmemStatus AssertPreClose();
  virtual PmemStatus Put(PmemKey key, PmemSlice value) = 0;
  virtual PmemStatus Merge(PmemKey key, PmemSlice value) = 0;
  virtual PmemStatus Delete(PmemKey key) = 0;
  virtual PmemStatus SingleDelete(PmemKey key) = 0;
  virtual PmemStatus DeleteRange(PmemKey start, PmemKey end) = 0;
  virtual PmemStatus CommitBatch(bool sync) = 0;
  virtual PmemStatus ApplyBatchRepr(PmemSlice repr, bool sync) = 0;
  virtual PmemSlice BatchRepr() = 0;
  virtual PmemStatus Get(PmemKey key, PmemString* value) = 0;
  virtual PmemIterator* NewIter(PmemIterOptions) = 0;
  virtual PmemStatus GetStats(PmemStatsResult* stats) = 0;
  virtual PmemStatus GetTickersAndHistograms(PmemTickersAndHistogramsResult* stats) = 0;
  virtual PmemString GetCompactionStats() = 0;
  virtual PmemString GetEnvStats(PmemEnvStatsResult* stats) = 0;
  virtual PmemStatus GetEncryptionRegistries(PmemEncryptionRegistries* result) = 0;
  virtual PmemStatus EnvWriteFile(PmemSlice path, PmemSlice contents) = 0;
  virtual PmemStatus EnvOpenFile(PmemSlice path, rocksdb::WritableFile** file) = 0;
  virtual PmemStatus EnvReadFile(PmemSlice path, PmemSlice* contents) = 0;
  virtual PmemStatus EnvAppendFile(rocksdb::WritableFile* file, PmemSlice contents) = 0;
  virtual PmemStatus EnvSyncFile(rocksdb::WritableFile* file) = 0;
  virtual PmemStatus EnvCloseFile(rocksdb::WritableFile* file) = 0;
  virtual PmemStatus EnvDeleteFile(PmemSlice path) = 0;
  virtual PmemStatus EnvDeleteDirAndFiles(PmemSlice dir) = 0;
  virtual PmemStatus EnvLinkFile(PmemSlice oldname, PmemSlice newname) = 0;

  PmemSSTable* GetSSTables(int* n);
  PmemStatus GetSortedWALFiles(PmemWALFile** out_files, int* n);
  PmemString GetUserProperties();
};

namespace cockroach {

struct EnvManager;

struct PmemImpl : public PmemEngine {
  std::unique_ptr<EnvManager> env_mgr;
  std::unique_ptr<rocksdb::Pmem> rep_deleter;
  std::shared_ptr<rocksdb::Cache> block_cache;
  std::shared_ptr<PmemEventListener> event_listener;
  std::atomic<int64_t> iters_count;

  // Construct a new PmemImpl from the specified Pmem.
  // The Pmem and passed Envs will be deleted when the PmemImpl is deleted.
  // Either env can be NULL.
  PmemImpl(rocksdb::Pmem* r, std::unique_ptr<EnvManager> e, std::shared_ptr<rocksdb::Cache> bc,
         std::shared_ptr<PmemEventListener> event_listener);
  virtual ~PmemImpl();

  virtual PmemStatus AssertPreClose();
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

}  // namespace cockroach
