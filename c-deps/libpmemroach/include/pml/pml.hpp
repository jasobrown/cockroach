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

#include "libpmemroach.h"
#include "pml/pool.hpp"
#include "pml/task.hpp"

using namespace pmem::obj;

namespace pml {

struct QueueContext {
    std::shared_ptr<folly::MPMCQueue<Task>> queue;
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
    void dispatch(Task &&t) noexcept;

    // folly::Future<int> move_range(low_key, high_key);
    // folly::Future<int> range_size(low_key);
    // folly::Future<PmemStatus> clean_range();

  private:
    std::vector<QueueContext> queueContexts;
    std::vector<std::shared_ptr<pool<PoolRoot>>> pools;
};
} // namespace pml


    /*
      PmemGet(PmemKey, DBString *) { // DBString is an out value;
        // need reference to disptacher
        Future<T> f  = dispatch.dispatch(key, [key] ()doPmemRead);
        DBString == transform(f.get());
      }

      template<typename T>
      Future<T> Dispatcher::dispatch(DBKey, std::fn<T(pool, promise)>) {
      auto promiseFuture = makeContract<String>()
        auto promise = std::get(1);
        queue.blockingWrite([&] { doPmemRead(key, promise);   } // promise *must* be fulfilled on the consumer thread
        return std::get(0);
        // copy out to DBString or whatever to return ...
      }
      
      // on the consumer thread
      doPmemRead(PmemKey key, pool<PoolRoot> pool, Promise p) {
        // do the real read from pmem here ... need pointer to
      pool/root of primary index.
        p.set_value(key.reverse());
      }
     */
