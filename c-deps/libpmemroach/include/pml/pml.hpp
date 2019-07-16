// Copyright 2019 Netflix
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

#include <thread>
#include <vector>

#include <folly/MPMCQueue.h>
#include <folly/futures/Future.h>
#include <folly/futures/Future-inl.h>
#include <folly/futures/Promise.h>

#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>

#include "libpmemroach.h"
#include "libpmemobj++/experimental/concurrent_hash_map.hpp"

using namespace pmem::obj;

namespace pml {

/// Modeled after the pool root structure as implemented
/// the java llpl library. (Note: this format isn't documented
/// in the llpl, but here we are.) The format contains two
/// unsigned 64-bit values: one that points to the user's
/// definded root object, and one that declares the intended
/// size of the heap. 
class PoolRoot {
  public:
    // TODO(jeb): decide what to do with ctor/copy ctor/move, etc...
    
    size_t GetUserRootOffset();
    void SetUserRootOffset(size_t size);

    /// Retrieve the size of the heap as declared when the heap
    /// was initially created.
    size_t GetHeapSize();

    //    persistent_ptr<PoolUserRoot> GetUserRoot();

  private:
    p<uint64_t> userRootOffset;
    p<uint64_t> heapSize;
    //    persistent_ptr<PoolUserRoot> userRoot;
};

enum class Action { READ, WRITE };

struct DispatchEvent {
    Action action;
    int value;
    folly::Promise<int> promise;
};
    
struct QueueContext {
    std::shared_ptr<folly::MPMCQueue<DispatchEvent>> queue;
    std::thread *queueConsumerThread;
};

class PmemContext {
  public:
    static
    std::shared_ptr<PmemContext> createAndInit();
    // TODO(jeb): add ctor/dtor/overrides for copy-ctor, and so on

    //management functions
    folly::Future<PmemStatus> shutdown();

    // behavioral functions
    folly::Future<int> dispatch();

    // folly::Future<int> move_range(low_key, high_key);

    // folly::Future<int> range_size(low_key);

    // folly::Future<PmemStatus> clean_range();

  private:
    std::vector<QueueContext> queueContexts;
};
} // namespace pml
