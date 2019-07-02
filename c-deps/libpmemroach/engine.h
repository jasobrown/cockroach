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
#include <libpmemroach.h>

struct PmemEngine {
  std::atomic<int64_t>* iters;

    PmemEngine()  {};
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
};

namespace cockroach {

struct PmemImpl : public PmemEngine {
  std::atomic<int64_t> iters_count;

  // Construct a new PmemImpl from the specified Pmem.
  // The Pmem and passed Envs will be deleted when the PmemImpl is deleted.
  // Either env can be NULL.
    PmemImpl();
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
};

}  // namespace cockroach
