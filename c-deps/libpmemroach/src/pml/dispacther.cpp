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

void setAffinity(int cpuIndex) {
    cpu_set_t cs;
    CPU_ZERO(&cs);
    CPU_SET(cpuIndex, &cs);
    auto r = pthread_setaffinity_np(pthread_self(), sizeof(cs), &cs);
    if (r != 0) {
        std::cout << "failed to assign thread affinity for cpuIndex " << cpuIndex << ", ret code = " << r << std::endl;
    }
}

QueueContext buildQueueContext(std::shared_ptr<pool<PoolRoot>> pool, int cpuId) {
    auto queue = std::make_shared<folly::MPMCQueue<Task>>(folly::MPMCQueue<Task>(128));
    std::thread *consumer = new std::thread([cpuId, queue, pool] {
        setAffinity(cpuId);
        consume(queue, pool);
     });

    return QueueContext{queue, consumer};
}

PoolContext openPool(PmemPoolConfig &config, std::vector<int> cpuIds) {
    auto pop = pool<PoolRoot>::open(config.path, LAYOUT_NAME);
    // read existing root object
    // assert that stored size == size argument
    auto pool_ptr = std::make_shared<pool<PoolRoot>>(pop);

    return PoolContext{pool_ptr, std::vector<TreeContext>()};
}

PoolContext createPool(PmemPoolConfig &config, std::vector<int> cpuIds) {
    auto pop = std::make_shared<pool<PoolRoot>>(pool<PoolRoot>::create(config.path, LAYOUT_NAME, config.size));
    auto root = pop->root();

    PoolContext ctx;
    transaction::run(*pop, [&ctx, root, pop, cpuIds] {
        root->heapSize = PMEMOBJ_MIN_POOL;
        pop->persist(root->heapSize);
        root->userRootOffset = 0;
        pop->persist(root->userRootOffset);

        std::vector<TreeContext> trees;
        
        // next, setup the trees
        for (const int cpuId : cpuIds) {
            auto treeRoot = nullptr;
            auto queueContext = buildQueueContext(pop, cpuId);
            trees.emplace_back(TreeContext{treeRoot, queueContext});
        }
        
        // TODO(jeb): set up the user root, with pointer to map that
        // points to all the trees

        ctx = PoolContext{pop, trees};
    });

    return ctx;
}

PoolContext loadPool(PmemPoolConfig &config, std::vector<int> cpuIds) {
    // do a check to see if file exists, instead of blindly creating
    std::ifstream poolCheck(config.path);
    if (poolCheck) {
        return openPool(config, cpuIds);
    }

    return createPool(config, cpuIds);
}

int cpuCount() {
    hwloc_topology_t topology;
    hwloc_topology_init(&topology);
    hwloc_topology_load(topology);
    int cpuCount = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_PU);
    hwloc_topology_destroy(topology);
    return cpuCount;
}

std::shared_ptr<PmemContext> PmemContext::createAndInit() {
    // setup pmem pools
    std::vector<PmemPoolConfig> configs = getPoolConfigs(); 
    std::vector<std::shared_ptr<pool<PoolRoot>>> pools;

    // TODO(jeb): need to know number of cpus (that are local to numa nodes we
    // are actually going to use). for now, i'm just faking it ...
    int cpus = cpuCount();
    int cpusPerPool = cpus / pools.size();

    std::vector<PoolContext> poolCxts;
    for (int i = 0; i < configs.size(); ++i) {
        int baseCpuOffset = i * cpusPerPool;
        std::vector<int> cpuIds;
        for (int j = 0; j < cpusPerPool; ++j) {
            cpuIds.emplace_back(baseCpuOffset + j);
        }

        poolCxts.emplace_back(loadPool(configs[i], cpuIds));
    }
    
    return std::make_shared<PmemContext>(PmemContext{poolCxts});;
}

void PmemContext::dispatch(Task &&t) noexcept {
    // find correct pool/range entry
    // ... which then maps to a queue (maybe QueueContext).

    // TODO(jeb) stop faking the funk
    auto pool = pools_[0];
    //    auto cxt = queueContexts[0];

    //    cxt.queue->blockingWrite(t);
}




} // namespace pml
