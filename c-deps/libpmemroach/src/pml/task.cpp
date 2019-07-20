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

#include <iostream>

#include "pml/task.hpp"

namespace pml {

ReadTask::ReadTask() : Task() {
}

ReadTask::ReadTask(ReadTask &task) noexcept {
    promise_ = std::move(task.promise_);
}

ReadTask::ReadTask(ReadTask &&task) noexcept {
    promise_ = std::move(task.promise_);
}

ReadTask& ReadTask::operator=(ReadTask &task) noexcept {
    promise_ = std::move(task.promise_);
    return *this;
}

ReadTask& ReadTask::operator=(ReadTask &&task) noexcept {
    promise_ = std::move(task.promise_);
    return *this;
}

void ReadTask::exec() noexcept {
    std::cout << "in ReadTask::exec" << std::endl;
    promise_.setValue(ReadResponse{42});
}


WriteTask::WriteTask() : Task() {
}

WriteTask::WriteTask(WriteTask &task) noexcept {
    promise_ = std::move(task.promise_);
}

WriteTask::WriteTask(WriteTask &&task) noexcept {
    promise_ = std::move(task.promise_);
}

WriteTask& WriteTask::operator=(WriteTask &task) noexcept {
    promise_ = std::move(task.promise_);
    return *this;
}

WriteTask& WriteTask::operator=(WriteTask &&task) noexcept {
    promise_ = std::move(task.promise_);
    return *this;
}

void WriteTask::exec() noexcept {
    std::cout << "in WriteTask::exec" << std::endl;
    promise_.setValue(true);
}

} // namespace pml
