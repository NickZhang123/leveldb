// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;  // kFilterBase = 2K

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

// 生成filter block的中的filter data和filter offset array
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  // 每2k生成一个bloom filter offset
  uint64_t filter_index = (block_offset / kFilterBase);  // kFilterBase = 2K
  assert(filter_index >= filter_offsets_.size());

  // 相当于每2k都有一个offset，但是filter data没有filter offset数量多（累计n个2k生成一个filter data时）
  while (filter_index > filter_offsets_.size()) {
    // 可能多次调用（block对应的第一个offset保存filter data的起始位置，后续offset保存filter data的结束位置）
    GenerateFilter();
  }
}

// filter添加key，key加入string keys_, 每次保存前一次的keys_.size()到start_
void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());  
  keys_.append(k.data(), k.size());  
}

Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  // 1. 写入每个过滤器的offset
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);  // 固定32位，根据起始和结束位置可计算offset个数
  }

  // 2. 写入过滤器offset数组的偏移位置，固定32位，后续解析
  PutFixed32(&result_, array_offset);

  // 3. 追加kFilterBaseLg = 11
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result

  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {
  // 1. vector start_保存每次加入key的位置; 获取key个数
  const size_t num_keys = start_.size();  

  // 2. 如果key数量为0，则对应的过滤器内容为上次过滤器末尾（这使得filter block中的filter data数量小于filter offset数量）
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // 3. 导出每个key
  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation  // 用于后续计算最后一个元素长度

  // 从string keys_中切分每个key出来保存至tmp_keys_
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // 4. 加入filter data前，先加入filter offset
  // Generate filter for current set of keys and append to result_.
  filter_offsets_.push_back(result_.size());

  // 5. 计算filter data并加入result_
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {

  // 1. 判断内容，因为每个filter最基本包含offset array offset（4） 和 标志kFilterBaseLg（1）
  // 因此最少应该有5个字节
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array

  // 2. 获取标志，即 kFilterBaseLg = 11
  base_lg_ = contents[n - 1];

  // 3. 解析偏移数组的起始位置，并判断起始位置的有效性
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;

  // 4. 获得offset_数组地址
  data_ = contents.data();
  offset_ = data_ + last_word;      // offset_数组地址
  num_ = (n - 5 - last_word) / 4;   // offset_数组元素个数
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  // 1. 计算offset index
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    // 2. start和limit为其offser index地址范围
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);

    // 3. 参数block_offset一般是data_block大小>=4k，offset数组一般是2k建立一个
    // 因此计算出来的start是这组data_block对应filter_data的起始位置，limit则是filter_data的结束位置
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
