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

#include "engine.h"
#include "db.h"
//#include "encoding.h"
#include "fmt.h"
#include "iterator.h"
//#include "protos/storage/engine/enginepb/rocksdb.pb.h"
#include "status.h"

using namespace cockroach;

PmemEngine::~PmemEngine() {}

PmemStatus PmemEngine::AssertPreClose() { return kSuccess; }

namespace cockroach {

PmemImpl::PmemImpl() {

}

PmemImpl::~PmemImpl() {
}

PmemStatus PmemImpl::AssertPreClose() {
    return kSuccess;
}

PmemStatus PmemImpl::Put(PmemKey key, PmemSlice value) {
    return kSuccess;
}

PmemStatus PmemImpl::Merge(PmemKey key, PmemSlice value) {
    return kSuccess;
}

PmemStatus PmemImpl::Get(PmemKey key, PmemString* value) {
    

    return kSuccess;
}

PmemStatus PmemImpl::Delete(PmemKey key) {
    return kSuccess;
}

PmemStatus PmemImpl::SingleDelete(PmemKey key) {
    return kSuccess;
}

PmemStatus PmemImpl::DeleteRange(PmemKey start, PmemKey end) {
    return kSuccess;
}

PmemStatus PmemImpl::CommitBatch(bool sync) { return kSuccess; }

PmemStatus PmemImpl::ApplyBatchRepr(PmemSlice repr, bool sync) {
    return kSuccess;
}

PmemSlice PmemImpl::BatchRepr() { return ToPmemSlice(kSuccess); }

PmemIterator* PmemImpl::NewIter(PmemIterOptions iter_opts) {
  PmemIterator* iter = new PmemIterator(iters, iter_opts);
  //  iter->rep.reset(rep->NewIterator(iter->read_opts));
  return iter;
}

}  // namespace cockroach
