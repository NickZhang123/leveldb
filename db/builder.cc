// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

// immutable->L0 SST, 构建sst文件
Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
  Status s;
  meta->file_size = 0;

  // 迭代器使用前首先调用SeekToFirst，迭代memtable中的内容
  iter->SeekToFirst();  

  // 构建sst文件名
  std::string fname = TableFileName(dbname, meta->number);

  // 创建sst文件
  if (iter->Valid()) {
    WritableFile* file;
    // 创建一个本地文件
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    // 使用TableBuilder构建SST
    TableBuilder* builder = new TableBuilder(options, file);

    // 保存sst最小值
    meta->smallest.DecodeFrom(iter->key());

    // 遍历memtable，加入builder
    Slice key;
    for (; iter->Valid(); iter->Next()) {
      key = iter->key();
      builder->Add(key, iter->value()); // 构建SST data_block
    }

    // 保留sst最大值
    if (!key.empty()) {
      meta->largest.DecodeFrom(key);
    }

    // Finish and check for builder errors
    s = builder->Finish();  // 构建SST 剩下block
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number, meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fname);
  }
  return s;
}

}  // namespace leveldb
