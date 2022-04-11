// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>

#include "proto/gen/db_operation.pb.h"
#include "rocksdb/file_system.h"
#include "rocksdb/sst_file_writer.h"
#include "src/libfuzzer/libfuzzer_macro.h"
#include "table/table_reader.h"
#include "util.h"

using namespace ROCKSDB_NAMESPACE;

// Keys in SST file writer operations must be unique and in ascending order.
// For each DBOperation generated by the fuzzer, this function is called on
// it to deduplicate and sort the keys in the DBOperations.
protobuf_mutator::libfuzzer::PostProcessorRegistration<DBOperations> reg = {
    [](DBOperations* input, unsigned int /* seed */) {
      const Comparator* comparator = BytewiseComparator();
      auto ops = input->mutable_operations();

      // Make sure begin <= end for DELETE_RANGE.
      for (DBOperation& op : *ops) {
        if (op.type() == OpType::DELETE_RANGE) {
          auto begin = op.key();
          auto end = op.value();
          if (comparator->Compare(begin, end) > 0) {
            std::swap(begin, end);
            op.set_key(begin);
            op.set_value(end);
          }
        }
      }

      std::sort(ops->begin(), ops->end(),
                [&comparator](const DBOperation& a, const DBOperation& b) {
                  return comparator->Compare(a.key(), b.key()) < 0;
                });

      auto last = std::unique(
          ops->begin(), ops->end(),
          [&comparator](const DBOperation& a, const DBOperation& b) {
            return comparator->Compare(a.key(), b.key()) == 0;
          });
      ops->erase(last, ops->end());
    }};

TableReader* NewTableReader(const std::string& sst_file_path,
                            const Options& options,
                            const EnvOptions& env_options,
                            const ImmutableCFOptions& cf_ioptions) {
  // This code block is similar to SstFileReader::Open.

  uint64_t file_size = 0;
  std::unique_ptr<RandomAccessFile> file;
  std::unique_ptr<RandomAccessFileReader> file_reader;
  std::unique_ptr<TableReader> table_reader;
  Status s = options.env->GetFileSize(sst_file_path, &file_size);
  if (s.ok()) {
    s = options.env->NewRandomAccessFile(sst_file_path, &file, env_options);
  }
  if (s.ok()) {
    file_reader.reset(new RandomAccessFileReader(
        NewLegacyRandomAccessFileWrapper(file), sst_file_path));
  }
  if (s.ok()) {
    TableReaderOptions t_opt(cf_ioptions, /*prefix_extractor=*/nullptr,
                             env_options, cf_ioptions.internal_comparator);
    t_opt.largest_seqno = kMaxSequenceNumber;
    s = options.table_factory->NewTableReader(t_opt, std::move(file_reader),
                                              file_size, &table_reader,
                                              /*prefetch=*/false);
  }
  if (!s.ok()) {
    std::cerr << "Failed to create TableReader for " << sst_file_path << ": "
              << s.ToString() << std::endl;
    abort();
  }
  return table_reader.release();
}

ValueType ToValueType(OpType op_type) {
  switch (op_type) {
    case OpType::PUT:
      return ValueType::kTypeValue;
    case OpType::MERGE:
      return ValueType::kTypeMerge;
    case OpType::DELETE:
      return ValueType::kTypeDeletion;
    case OpType::DELETE_RANGE:
      return ValueType::kTypeRangeDeletion;
    default:
      std::cerr << "Unknown operation type " << static_cast<int>(op_type)
                << std::endl;
      abort();
  }
}

// Fuzzes DB operations as input, let SstFileWriter generate a SST file
// according to the operations, then let TableReader read and check all the
// key-value pairs from the generated SST file.
DEFINE_PROTO_FUZZER(DBOperations& input) {
  if (input.operations().empty()) {
    return;
  }

  std::string sstfile;
  {
    auto fs = FileSystem::Default();
    std::string dir;
    IOOptions opt;
    CHECK_OK(fs->GetTestDirectory(opt, &dir, nullptr));
    sstfile = dir + "/SstFileWriterFuzzer.sst";
  }

  Options options;
  EnvOptions env_options(options);
  ImmutableCFOptions cf_ioptions(options);

  // Generate sst file.
  SstFileWriter writer(env_options, options);
  CHECK_OK(writer.Open(sstfile));
  for (const DBOperation& op : input.operations()) {
    switch (op.type()) {
      case OpType::PUT: {
        CHECK_OK(writer.Put(op.key(), op.value()));
        break;
      }
      case OpType::MERGE: {
        CHECK_OK(writer.Merge(op.key(), op.value()));
        break;
      }
      case OpType::DELETE: {
        CHECK_OK(writer.Delete(op.key()));
        break;
      }
      case OpType::DELETE_RANGE: {
        CHECK_OK(writer.DeleteRange(op.key(), op.value()));
        break;
      }
      default: {
        std::cerr << "Unsupported operation" << static_cast<int>(op.type())
                  << std::endl;
        abort();
      }
    }
  }
  ExternalSstFileInfo info;
  CHECK_OK(writer.Finish(&info));

  // Iterate and verify key-value pairs.
  std::unique_ptr<TableReader> table_reader(
      NewTableReader(sstfile, options, env_options, cf_ioptions));
  ReadOptions roptions;
  CHECK_OK(table_reader->VerifyChecksum(roptions,
                                        TableReaderCaller::kUncategorized));
  std::unique_ptr<InternalIterator> it(
      table_reader->NewIterator(roptions, /*prefix_extractor=*/nullptr,
                                /*arena=*/nullptr, /*skip_filters=*/true,
                                TableReaderCaller::kUncategorized));
  it->SeekToFirst();
  for (const DBOperation& op : input.operations()) {
    if (op.type() == OpType::DELETE_RANGE) {
      // InternalIterator cannot iterate over DELETE_RANGE entries.
      continue;
    }
    CHECK_TRUE(it->Valid());
    ParsedInternalKey ikey;
    CHECK_OK(ParseInternalKey(it->key(), &ikey, /*log_err_key=*/true));
    CHECK_EQ(ikey.user_key.ToString(), op.key());
    CHECK_EQ(ikey.sequence, 0);
    CHECK_EQ(ikey.type, ToValueType(op.type()));
    if (op.type() != OpType::DELETE) {
      CHECK_EQ(op.value(), it->value().ToString());
    }
    it->Next();
  }
  CHECK_TRUE(!it->Valid());

  // Delete sst file.
  remove(sstfile.c_str());
}
