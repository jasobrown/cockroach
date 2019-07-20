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

#include <folly/futures/Promise.h>

#include "libpmemroach.h"
#include "pml/pool.hpp"

using namespace pmem::obj;

namespace pml {

// TODO(jeb) replace me
class ReadResponse {
  public:
    int f;
};

class Task {
  public:
    Task() = default;
    Task(Task &task) noexcept = default;
    Task(Task &&task) noexcept = default;
    virtual Task& operator=(Task &task) noexcept = default;
    virtual Task& operator=(Task &&task) noexcept = default;

    // cannot make this a pure virtual function else we get compiler
    // nastiness like this: "static_assert failed "T must be
    // relocatable or have a noexcept move constructor", and I do not
    // know how to fix that. However, by having a non-pure virutal
    // function (a/k/a give it a nop implementation), it seems to make
    // the compiler happy.
    virtual void exec() noexcept { };

  protected:
    PmemKey key;
    std::shared_ptr<pool<PoolRoot>> pool;
};

class ReadTask : public Task {
  public :
    ReadTask();
    ReadTask(folly::Promise<ReadResponse> p) : promise_(std::move(p)) {}
    ReadTask(ReadTask &task) noexcept;
    ReadTask(ReadTask &&task) noexcept;
    ReadTask& operator=(ReadTask &task) noexcept;
    ReadTask& operator=(ReadTask &&task) noexcept;

    void exec() noexcept;

  private:
    folly::Promise<ReadResponse> promise_;
};

class WriteTask : public Task {
  public :
    WriteTask();
    WriteTask(folly::Promise<bool> p) : promise_(std::move(p)) {}
    WriteTask(WriteTask &task) noexcept;
    WriteTask(WriteTask &&task) noexcept;
    WriteTask& operator=(WriteTask &task) noexcept;
    WriteTask& operator=(WriteTask &&task) noexcept;

    void exec() noexcept;

  private:
    // TODO(jeb) not really using the bool here, I just need a 'void'
    // return type. the folly docs state I can use a 'folly::Unit', but i
    // haven't figured that out yet.
    folly::Promise<bool> promise_;
};

} // namespace pml
