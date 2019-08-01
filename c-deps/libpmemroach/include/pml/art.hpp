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

#include "libpmemroach.h"
#include "pml/pool.hpp"

using namespace pmem::obj;

// TODO(jeb) unclear if I need to declare the node types in the header

/// An implementation of the Adaptive Radix Tree (ART) paper
/// (https://db.in.tum.de/~leis/papers/ART.pdf).
///
/// Physical layout of the node is like this:
/// +--------+---------------------------------------+--------+
/// | header |             data                      | prefix |
/// +--------+---------------------------------------+--------+
///
/// header, 4 bytes:
/// +-------+---------+--------------+-------------+------------+
/// |version|node-type|  prefix-len  | child-count | cur-prefix |
/// +(4bits)| (4bits) |    (1 byte)  |  (1 byte)   |  (1 byte)  |
/// +-------+---------+--------------+-------------+------------+
///
/// - node-type is one of the four types as defined in the ART paper.
/// - As we always have a least one byte for a prefix (and commonly
///   only one), the first byte is stored in the header in the
///   cur-prefix field. Additional preifx bytes, if using path
///   compression, are stored in the optional prefix section at
///   the end of the block. The prefix-len field, then, is always
///   greater than or equal to 1.
/// - child-count field indicates how many of the node's slots are
///   occupied.
///
/// data (size depends on node type):
/// Implemention of the different node-types is as described in the
/// ART paper.
///
/// prefix:
/// An optional section if the prefix for this node cannot fit in the
/// header, meaning that the prefix is greater than one byte.


namespace pml {

namespace art {

/// Wrapper for logic and serialization of the ART. Does not maintain
/// stored data itself, but pointers to data.
class ArtTree {
  public:
    ArtTree(std::shared_ptr<pool<PoolRoot>> pool) : pool_(pool) {}


    static std::shared_ptr<ArtTree>
    createTree(std::shared_ptr<pool<PoolRoot>> pool);

    void search(PmemKey key);
    void insert(PmemKey key /* value */);
    //void delete(PmemKey key);
    
  protected:
    std::shared_ptr<pool<PoolRoot>> pool_;
};


    
} // namespace art

} // namespace pml


