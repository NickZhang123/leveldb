// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

#include <cstdint>
#include <vector>

#include "leveldb/slice.h"

namespace leveldb {

struct Options;
/*
BlockBuilder用于构建data_block，有两个主要成员，string buffer_和vector<int> restarts_
  buffer_：逐字节保存k-v，格式为（shared_key_len + non_shared_key_len + val_len + non_shared_key + val)，使用前缀压缩算法
      shared_key_len：当前key与前一个key的相同前缀长度，PutVarint32变长保存
      non_shared_key_len：当前key与前一个key除去相同前缀后的非相同部分长度，PutVarint32变长保存
      val_len：value长度，PutVarint32变长保存
      non_shared_key：除去相同前缀后的非相同部分key
      val：传入的value值
  restarts_: 没输入16个k-v，则建立一个重启点，restarts_中保存buffer_中的位置作为重启点，便于快速恢复、解析压缩数据

数据一致保存在内存中，最后下盘时，将restarts_中保存的数据以固定32位每个重启点的形式追加到buffer_, 追加完重启点后，再追加一个重启点数量，以便解析重启点， 然后将整个buffer_内容下盘

每个block大小默认配置为4k，大于4k就刷新一个block，同时添加一个kv进data_index

 */



class BlockBuilder {
 public:
  explicit BlockBuilder(const Options* options);

  BlockBuilder(const BlockBuilder&) = delete;
  BlockBuilder& operator=(const BlockBuilder&) = delete;

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return buffer_.empty(); }

 private:
  const Options* options_;
  std::string buffer_;              // Destination buffer 叠加保存shared_len + non_shared_len + value_len + un_shared_key + val
  std::vector<uint32_t> restarts_;  // Restart points     重启点，为buffer_.size()
  int counter_;                     // Number of entries emitted since restart  记录当前数量，每16个加入一个重启点
  bool finished_;                   // Has Finish() been called?
  std::string last_key_;  // 上次写入的key
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
