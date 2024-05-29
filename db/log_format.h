// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.md for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

namespace leveldb {
namespace log {


// 如果记录能装入block，则记录形成一个trunk，trunk类型为full
// 如果记录太大，不止占用一个block，则每个block形成一个trunk记录
//    第一个block中的trunk类型为first，中间block的trunk类型为middle，最后一个block的trunk类型为last
// RecordType理解为上述trunk类型
enum RecordType {  
  // Zero is reserved for preallocated files
  kZeroType = 0,

  kFullType = 1,  // 记录能在一个block中容纳

  // For fragments
  kFirstType = 2,   // 记录大于block剩余空间且是记录头部，则trunk类型为first
  kMiddleType = 3,  
  kLastType = 4
};
static const int kMaxRecordType = kLastType;

static const int kBlockSize = 32768;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
static const int kHeaderSize = 4 + 2 + 1;

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
