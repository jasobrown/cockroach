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

#include "pml/art.hpp"

namespace pml {

namespace art {




std::shared_ptr<ArtTree>
ArtTree::createTree(std::shared_ptr<pool<PoolRoot>> pool) {
    return std::make_shared<ArtTree>(ArtTree{pool});
}

} //namespace art 

} // namepsace pml

