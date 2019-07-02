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

#include "encoding.h"
#include "iterator.h"
#include <atomic>

using namespace cockroach;

PmemIterator::PmemIterator(std::atomic<int64_t>* iters, PmemIterOptions iter_options) : iters_count(iters) {

}

PmemIterator::~PmemIterator() {
}

void PmemIterator::SetLowerBound(PmemKey key) {
}


void PmemIterator::SetUpperBound(PmemKey key) {

}
