// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <cstdint>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

// 从block offset为0处开始写
Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}

// 指定block offset位置写
Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;

// 写一个log，log格式为 header(crc+len+type) + data
Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {

    // 1. 计算当前block剩余空间
    const int leftover = kBlockSize - block_offset_;  // kBlockSize = 32k, 
    assert(leftover >= 0);

    // 2. 日志block大小为32k，如果剩下block大小不满足一条记录的header，则换一个block
    if (leftover < kHeaderSize) {
      // Switch to a new block
      // 补零后切换
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7, "");
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    // 3. 计算block剩下能保存的data长度； 
    // left为记录剩余长度，avail为block剩余长度
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    const size_t fragment_length = (left < avail) ? left : avail;

    // 4. 计算log类型，分为full，first、mid、last
    RecordType type;
    const bool end = (left == fragment_length);  // 如果当前block能够容纳记录，则end为true，当前记录类型为full
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;    // 记录第一个trunk类型
    } else if (end) {
      type = kLastType;     // 记录最后一个trunk类型
    } else {
      type = kMiddleType;   // 记录中间类型
    }

    // 5. 写入log至block中，实质上写入对应的文件中（不需要凑满一个block写入）
    s = EmitPhysicalRecord(type, ptr, fragment_length);


    // 6. 更新已写入的数据
    ptr += fragment_length;   // 要写入的记录位置
    left -= fragment_length;  // 剩下记录内容
    begin = false;
  } while (s.ok() && left > 0);
  
  return s;
}

// 组装header+data格式后下盘，不需要抽满一个block才下盘
// header: crc(4) + len(2) + type(1)
// 先写header，再写val（type + var_key_len + key + var_val_len + val）
// 更新block已使用长度（用于逻辑上block划分
Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t length) {
  assert(length <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);

  // Format the header
  // 1. 组装header+data格式的log
  char buf[kHeaderSize];
  buf[4] = static_cast<char>(length & 0xff);
  buf[5] = static_cast<char>(length >> 8);
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  crc = crc32c::Mask(crc);  // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  // 2. 分别将header和data写入文件系统，并刷新
  Status s = dest_->Append(Slice(buf, kHeaderSize));  // 先写header
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, length));            // 再写val（type + var_key_len + key + var_val_len + val）
    if (s.ok()) {
      s = dest_->Flush();
    }
  }

  // 3. 增加block offset计数
  // 注意：不论写成功和失败都增加
  block_offset_ += kHeaderSize + length;
  
  return s;
}

}  // namespace log
}  // namespace leveldb
