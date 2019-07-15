// Copyright 2017 The Cockroach Authors.
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

#include <libpmemroach.h>
#include <memory>

namespace cockroach {

inline PmemSlice ToPmemSlice(const PmemString& s) {
  PmemSlice result;
  result.data = s.data;
  result.len = s.len;
  return result;
}

// ToString converts a PmemSlice/PmemString to a C++ string.
inline std::string ToString(PmemSlice s) { return std::string(s.data, s.len); }
inline std::string ToString(PmemString s) { return std::string(s.data, s.len); }

// // MVCCComputeStatsInternal returns the mvcc stats of the data in an iterator.
// // Stats are only computed for keys between the given range.
// MVCCStatsResult MVCCComputeStatsInternal(::rocksdb::Iterator* const iter_rep, PmemKey start,
//                                          PmemKey end, int64_t now_nanos);

// ScopedStats wraps an iterator and, if that iterator has the stats
// member populated, aggregates a subset of the RocksPmem perf counters
// into it (while the ScopedStats is live).
class ScopedStats {
 public:
  ScopedStats(PmemIterator*);
  ~ScopedStats();

 private:
  PmemIterator* const iter_;
  uint64_t internal_delete_skipped_count_base_;
};

// // BatchSStables batches the supplied sstable metadata into chunks of
// // sstables that are target_size. An empty start or end key indicates
// // that the a compaction from the beginning (or end) of the key space
// // should be provided. The sstable metadata must already be sorted by
// // smallest key.
// void BatchSSTablesForCompaction(const std::vector<rocksdb::SstFileMetaData>& sst,
//                                 rocksdb::Slice start_key, rocksdb::Slice end_key,
//                                 uint64_t target_size, std::vector<rocksdb::Range>* ranges);

}  // namespace cockroach
