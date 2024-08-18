// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  FilterBlockBuilder(const FilterBlockBuilder&) = delete;
  FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;

  // 指定offset生成filter_data和filter_offset
  void StartBlock(uint64_t block_offset);

  // 加入key
  void AddKey(const Slice& key);

  // sst写入filter_data数据
  Slice Finish();

 private:
  void GenerateFilter();

  const FilterPolicy* policy_;

  std::string keys_;             // Flattened key contents   每次添加的key(连续存储key)
  std::vector<size_t> start_;    // Starting index in keys_ of each key  每次添加key时，保存上一个key的末尾size（分割每个key）

  std::string result_;           // Filter data computed so far  最终要写入的过滤器内容

  std::vector<Slice> tmp_keys_;  // policy_->CreateFilter() argument  // 一个data_block中的key
  std::vector<uint32_t> filter_offsets_;  // 保存每个过滤器的偏移offset
};

class FilterBlockReader {
 public:
  // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);                    // 调用policy_的KeyMayMatch接口对比

 private:
  const FilterPolicy* policy_;
  const char* data_;    // Pointer to filter data (at block-start)              // filter data起始位置
  const char* offset_;  // Pointer to beginning of offset array (at block-end)  // filter offset array 起始位置
  size_t num_;          // Number of entries in offset array                    // filter offser array 元素个数
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)   // 标识，也是filter计算的单位，默认为11，对应单位2k
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
