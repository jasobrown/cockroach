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

#include <fstream>
#include <iostream>
#include <vector>

#include <hwloc.h>
#include <folly/portability/Config.h>

#include "pml/pool.hpp"
#include "pml/pml.hpp"

using namespace pmem::obj;

#define LAYOUT_NAME "crdb_pmem_layout"

namespace pml {

// TODO(jeb): (might) need a mapping of NUMA node to local CPUs


struct PmemPoolConfig {
    const char *path;
    size_t size;
    int numaNode;
};

/// This function makes the wild (and possilbly untrue) assumption that
/// each NUMA node will only have one pool mapped to it. Further, we
/// assume each NUMA node on the machine has a pmem pool (otherwise, we
/// will have unused CPUs.
std::vector<PmemPoolConfig> getPoolConfigs() {
    // TODO(jeb): hard code pool information here
    std::vector<PmemPoolConfig> configs;
    configs.push_back(PmemPoolConfig{"/mnt/mem/pool0", PMEMOBJ_MIN_POOL, 0});
    configs.push_back(PmemPoolConfig{"/mnt/mem/pool1", PMEMOBJ_MIN_POOL, 1});

    return configs;
}

pool<PoolRoot> openPool(PmemPoolConfig &config) {
    auto pop = pool<PoolRoot>::open(config.path, LAYOUT_NAME);
    // read existing root object
    // assert that stored size == size argument
    return pop;
}

pool<PoolRoot> createPool(PmemPoolConfig &config) {
    auto pop = pool<PoolRoot>::create(config.path, LAYOUT_NAME, config.size);
    auto root = pop.root();

    transaction::run(pop, [&] {
        root->heapSize = PMEMOBJ_MIN_POOL;
        pop.persist(root->heapSize);
        root->userRootOffset = 0;
        pop.persist(root->userRootOffset);

        // TODO(jeb): set up the user root, once i know what that is ...
    });
    return pop;
}

std::shared_ptr<pool<PoolRoot>> loadPool(PmemPoolConfig &config) {
    // do a check to see if file exists, instead of blindly creating
    std::ifstream poolCheck(config.path);
    pmem::obj::pool<PoolRoot> pool;
    if (poolCheck) {
        pool = openPool(config);
    } else {
        pool = createPool(config);
    }
    
    // try using a custom Deleter on the shared_ptr to close the pool
    // before releasing memory. Not sure if this is the wrong thing to
    // do, but here we are ...
    return std::shared_ptr<::pool<PoolRoot>>(&pool, [](::pool<PoolRoot> *pool)
                                                    { pool->close(); delete pool; });
}

int cpuCount() {
    hwloc_topology_t topology;
    hwloc_topology_init(&topology);
    hwloc_topology_load(topology);
    int cpuCount = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_PU);
    hwloc_topology_destroy(topology);
    return cpuCount;
}

void setAffinity(int cpuIndex) {
    cpu_set_t cs;
    CPU_ZERO(&cs);
    CPU_SET(cpuIndex, &cs);
    auto r = pthread_setaffinity_np(pthread_self(), sizeof(cs), &cs);
    if (r != 0) {
        std::cout << "failed to assign thread affinity for cpuIndex " << cpuIndex << ", ret code = " << r << std::endl;
    }
}

/// A simple 'consume' function until I've fleshed out the downstream
/// consumer functionality/behaviors.
void consume(std::shared_ptr<folly::MPMCQueue<Task>> queue,
             std::shared_ptr<pool<PoolRoot>> pool) {
    while (true) {
        Task task;
        queue->blockingRead(task);
        task.exec();
    }
}


std::vector<std::shared_ptr<TreeManager>> initTrees(pool<PoolRoot> pool) {
    std::vector<std::shared_ptr<TreeManager>> trees;

    return trees;
}

std::shared_ptr<PmemContext>
PmemContext::createAndInit() {
    // setup pmem pools
    std::vector<PmemPoolConfig> configs = getPoolConfigs();
    std::vector<std::shared_ptr<pool<PoolRoot>>> pools;
    std::for_each(configs.begin(), configs.end(), [&] (auto&& config) { pools.push_back(loadPool(config));  });

    // set up the queues and consumer threads
    std::vector<QueueContext> queueContexts;
    // TODO(jeb): need to know number of cpus (that are local to numa nodes we
    // are actually going to use). for now, i'm just faking it ...
    int cpus = cpuCount();
    int cpusPerPool = cpus / pools.size();
    for (int i = 0; i < pools.size(); ++i) {
        auto pool = pools[i];
        int baseCpuOffset = i * cpusPerPool;
        for (int j = 0; j < cpusPerPool; ++j) {
            int cpuId = baseCpuOffset + j;
            auto queue = std::make_shared<folly::MPMCQueue<Task>>(folly::MPMCQueue<Task>(128));
            std::thread* consumer = new std::thread([cpuId, queue, pool] {
                setAffinity(cpuId);
                consume(queue, pool);
            });

            // TODO(jeb) FIX THIS ASAP
            queueContexts.push_back(QueueContext{queue, consumer});
        }
    }

    // set up ranges (possibly pre-partition ranges, not sure how to
    // do that), and map those to queues
    

    
    return nullptr;
}

void PmemContext::dispatch(Task &&t) noexcept {
    // find correct pool/range entry
    // ... which then maps to a queue (maybe QueueContext).

    // TODO(jeb) stop faking the funk
    auto pool = pools[0];
    auto cxt = queueContexts[0];

    cxt.queue->blockingWrite(t);
}









} // namespace pml
