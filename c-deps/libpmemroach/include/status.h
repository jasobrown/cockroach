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

#pragma once

#include "db.h"
#include "fmt.h"

namespace cockroach {

const PmemStatus kSuccess = {NULL, 0};

// ToPmemStatus converts a rocksdb Status to a PmemStatus.
// inline PmemStatus ToPmemStatus(const rocksdb::Status& status) {
//   if (status.ok()) {
//     return kSuccess;
//   }
//   return ToPmemString(status.ToString());
// }

// // FmtStatus formats the given arguments printf-style into a PmemStatus.
// __attribute__((__format__(GOOGLE_PRINTF_FORMAT, 1, 2))) inline PmemStatus
// FmtStatus(const char* fmt_str, ...) {
//   va_list ap;
//   va_start(ap, fmt_str);
//   std::string str;
//   fmt::StringAppendV(&str, fmt_str, ap);
//   va_end(ap);
//   return ToPmemString(str);
// }

}  // namespace cockroach
