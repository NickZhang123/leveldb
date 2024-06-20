// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <cstdio>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Reader::Reporter::~Reporter() = default;

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {}

Reader::~Reader() { delete[] backing_store_; }

bool Reader::SkipToInitialBlock() {
  // 1. 计算起始位置所在的block起始位置
  const size_t offset_in_block = initial_offset_ % kBlockSize;
  uint64_t block_start_location = initial_offset_ - offset_in_block;

  // 2. 如果起始位置在block中的最后不足一个header的位置，则跳过当前block
  // Don't search a block if we'd be in the trailer
  if (offset_in_block > kBlockSize - 6) {
    block_start_location += kBlockSize;
  }

  // 3. 
  end_of_buffer_offset_ = block_start_location;

  // 4. seek到文件指定位置
  // Skip to start of first block that can contain the initial record
  if (block_start_location > 0) {
    Status skip_status = file_->Skip(block_start_location);
    if (!skip_status.ok()) {
      ReportDrop(block_start_location, skip_status);
      return false;
    }
  }

  return true;
}

// record为读取的log，可能在block内，可能跨block
// 每次读取一个log，参数返回record保存log，参数scratch在跨block时保存拼接的数据
bool Reader::ReadRecord(Slice* record, std::string* scratch) {
  // 1. 判断如果有需要，则跳转到指定位置读取
  if (last_record_offset_ < initial_offset_) {
    if (!SkipToInitialBlock()) {
      return false;
    }
  }

  scratch->clear();
  record->clear();

  bool in_fragmented_record = false;  // 表示是否在读取跨block记录
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  uint64_t prospective_record_offset = 0;

  // 2. 读取文件中的记录
  Slice fragment;
  while (true) {
    // 2.1 解析一条log
    // 返回值为log类型，参数为log data
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    // ReadPhysicalRecord may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.
    // log在buffer中的起始offset，也是已经解析的最新日志的起始位置
    uint64_t physical_record_offset =
        end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size();   

    // 2.2 判断是否为重解析，默认为false
    if (resyncing_) {
      if (record_type == kMiddleType) {
        continue;
      } else if (record_type == kLastType) {
        resyncing_ = false;
        continue;
      } else {
        resyncing_ = false;
      }
    }

    switch (record_type) {
      // 2.3 日志类型不跨block，保存本次解析的日志至参数record并返回true
      case kFullType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }

        prospective_record_offset = physical_record_offset;
        scratch->clear();
        *record = fragment;  
        last_record_offset_ = prospective_record_offset;
        return true;

      // 2.4 日志类型为跨block，且为第一个，先保存至scratch中，并记录日志起始位置prospective_record_offset，继续读取
      case kFirstType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
        // 保存起始位置
        prospective_record_offset = physical_record_offset;

        // scratch中拼接每个frag data
        scratch->assign(fragment.data(), fragment.size());  

        in_fragmented_record = true;    // first处理后，表示处理跨block的log
        break;

      // 2.5 日志类型为跨block且为中间block，则拼接内容后继续读取
      case kMiddleType:
        if (!in_fragmented_record) {  
          // 如果处理到mid，则之前必须会处理first，处理first会置标志位true
          ReportCorruption(fragment.size(), "missing start of fragmented record(1)");
        } else {
          // 拼接frag
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      // 2.6 日志类型为跨block且最后一个block，则拼接数据后，保存整个record输出，并记录日志起始位置，返回true
      case kLastType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(), "missing start of fragmented record(2)");
        } else {
          // 拼接frag
          scratch->append(fragment.data(), fragment.size());

          // 输出拼接后的内容
          *record = Slice(*scratch);

          // 本次处理的log起始位置
          last_record_offset_ = prospective_record_offset;

          return true;
        }
        break;

      // 2.7 正常读完，或者日志读取失败，或者日志被截断； 返回false
      case kEof:
        if (in_fragmented_record) {  // 表示日志不完整
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return false;

      // 2.8 日志损害； 继续读下一个block
      case kBadRecord:
        if (in_fragmented_record) {  // 日志不完整
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        char buf[40];
        std::snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }

  }

  return false;
}

uint64_t Reader::LastRecordOffset() { return last_record_offset_; }

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != nullptr &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

// 返回log类型，result为解析出来的一个log data
unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  while (true) {
    // 1.如果当前block处理完后，重新读取一个block
    // 读取整个block后，可能读取失败，可能读取完
    if (buffer_.size() < kHeaderSize) {
      if (!eof_) {
        // Last read was a full read, so this is a trailer to skip
        buffer_.clear();

        // 顺序读取block 32k（读完后指针后移，下次读取下一个block）
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
        end_of_buffer_offset_ += buffer_.size();
        if (!status.ok()) {  
          // 读取失败
          buffer_.clear();
          ReportDrop(kBlockSize, status);
          eof_ = true;
          return kEof;
        } else if (buffer_.size() < kBlockSize) {
          // 读完
          eof_ = true;
        }
        continue;
      } else {
        // Note that if buffer_ is non-empty, we have a truncated header at the
        // end of the file, which can be caused by the writer crashing in the
        // middle of writing the header. Instead of considering this an error,
        // just report EOF.
        buffer_.clear();
        return kEof;
      }
    }

    // Parse the header
    // 2. 按照header+data的格式解析block中的log

    // 2.1 先解析header中的crc，len，type
    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);

    // 2.2 判断len，异常 kBadRecord
    if (kHeaderSize + length > buffer_.size()) {
      size_t drop_size = buffer_.size();
      buffer_.clear();  // 下次再读一个新的block
      if (!eof_) {
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord;
      }
      // If the end of the file has been reached without reading |length| bytes
      // of payload, assume the writer died in the middle of writing the record.
      // Don't report a corruption.
      return kEof;
    }

    // 2.3 判断type，异常kBadRecord
    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      buffer_.clear();
      return kBadRecord;
    }

    // 2.4 判断crc异常，Check crc， kBadRecord
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
      if (actual_crc != expected_crc) {
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }

    // 3. 解析完一个log，指针后移
    buffer_.remove_prefix(kHeaderSize + length);

    // Skip physical record that started before initial_offset_
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length < initial_offset_) {
      result->clear();
      return kBadRecord;
    }

    // 4. result保存解析出来的日志，只保存data部分
    *result = Slice(header + kHeaderSize, length);

    // 5. 返回解析出来的类型
    return type;
  }
}

}  // namespace log
}  // namespace leveldb
