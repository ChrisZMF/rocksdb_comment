//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "db/transaction_log_impl.h"
#include <cinttypes>
#include "db/write_batch_internal.h"
#include "file/sequence_file_reader.h"

namespace ROCKSDB_NAMESPACE {

TransactionLogIteratorImpl::TransactionLogIteratorImpl(
    const std::string& dir, const ImmutableDBOptions* options,
    const TransactionLogIterator::ReadOptions& read_options,
    const EnvOptions& soptions, const SequenceNumber seq,
    std::unique_ptr<VectorLogPtr> files, VersionSet const* const versions,
    const bool seq_per_batch, const std::shared_ptr<IOTracer>& io_tracer)
    : dir_(dir),
      options_(options),
      read_options_(read_options),
      soptions_(soptions),
      starting_sequence_number_(seq),
      files_(std::move(files)),   //files_就是传入的满足>=seq的文件的vector
      started_(false),
      is_valid_(false),
      current_file_index_(0),
      current_batch_seq_(0),
      current_last_seq_(0),
      versions_(versions),
      seq_per_batch_(seq_per_batch),
      io_tracer_(io_tracer) {
  assert(files_ != nullptr);
  assert(versions_ != nullptr);
  current_status_.PermitUncheckedError();  // Clear on start
  reporter_.env = options_->env;
  reporter_.info_log = options_->info_log.get();
  SeekToStartSequence(); // Seek till starting sequence
}

Status TransactionLogIteratorImpl::OpenLogFile(
    const LogFile* log_file,
    std::unique_ptr<SequentialFileReader>* file_reader) {
  FileSystemPtr fs(options_->fs, io_tracer_);
  std::unique_ptr<FSSequentialFile> file;
  std::string fname;
  Status s;
  EnvOptions optimized_env_options = fs->OptimizeForLogRead(soptions_); //FileSystem::OptimizeForLogRead
  if (log_file->Type() == kArchivedLogFile) { //打开archive中的wal
    fname = ArchivedLogFileName(dir_, log_file->LogNumber());
    s = fs->NewSequentialFile(fname, optimized_env_options, &file, nullptr);
  } else {  //打开db目录的wal
    fname = LogFileName(dir_, log_file->LogNumber());
    s = fs->NewSequentialFile(fname, optimized_env_options, &file, nullptr);
    if (!s.ok()) {  //如果失败，尝试在archive目录寻找
      //  If cannot open file in DB directory.
      //  Try the archive dir, as it could have moved in the meanwhile.
      fname = ArchivedLogFileName(dir_, log_file->LogNumber());
      s = fs->NewSequentialFile(fname, optimized_env_options,
                                &file, nullptr);
    }
  }
  if (s.ok()) {
    file_reader->reset(
        new SequentialFileReader(std::move(file), fname, io_tracer_));
  }
  return s;
}

BatchResult TransactionLogIteratorImpl::GetBatch()  {
  assert(is_valid_);  //  cannot call in a non valid state.
  BatchResult result;
  result.sequence = current_batch_seq_;
  result.writeBatchPtr = std::move(current_batch_);
  return result;
}

Status TransactionLogIteratorImpl::status() { return current_status_; }

bool TransactionLogIteratorImpl::Valid() { return started_ && is_valid_; }

bool TransactionLogIteratorImpl::RestrictedRead(Slice* record) {
  // Don't read if no more complete entries to read from logs
  if (current_last_seq_ >= versions_->LastSequence()) { //当前迭代的seq不能有误，迭代结束的sequence标志
    return false;
  }
  return current_log_reader_->ReadRecord(record, &scratch_);  //current_log_reader_在OpenLogReader中创建；record返回的是wal一个record中的WriteBatch信息
}

void TransactionLogIteratorImpl::SeekToStartSequence(uint64_t start_file_index,
                                                     bool strict) { //默认入参为：0，false
  Slice record;
  started_ = false;
  is_valid_ = false;
  if (files_->size() <= start_file_index) { //start_file_index可以是0
    return;
  }
    //若打开文件失败，则iter的is_valid_=false
  Status s =
      OpenLogReader(files_->at(static_cast<size_t>(start_file_index)).get()); //创建了current_log_reader_，在RestrictedRead中调用
  if (!s.ok()) {
    current_status_ = s;
    reporter_.Info(current_status_.ToString().c_str());
    return;
  }
  /*
    1、通过RestrictedRead使用log_reader从wal文件中依次读取出record
    2、UpdateCurrentWriteBatch中计算record中的seq值，赋值给current_batch_seq_，并按照每次读取的record的batch大小累加，
      并将当前的batch赋值给iter的current_batch_作为iter->GetBatch的返回内容；
    3、直到current_last_seq_ >= starting_sequence_number_（传入的起始seq），表明iter创建成功并可以用于迭代后续的wal中的内容；
    4、目前问题就在于RestrictedRead返回的record是什么？wal的存储结构，实际就是各种WriteBatch或merged WriteBatch
  */
  while (RestrictedRead(&record)) {   //读出wal中的record；record中是什么？record是wal存储结构，wal是一条条变长record存储的
    if (record.size() < WriteBatchInternal::kHeader) {
      reporter_.Corruption(
        record.size(), Status::Corruption("very small log record"));
      continue;
    }
    UpdateCurrentWriteBatch(record);
    if (current_last_seq_ >= starting_sequence_number_) { //此处为>=，则iter->GetBatch返回的current_batch_中的内容包含了目标seq所指定的内容，所以关键就在record中是什么？
      if (strict && current_batch_seq_ != starting_sequence_number_) {
        current_status_ = Status::Corruption(
            "Gap in sequence number. Could not "
            "seek to required sequence number");
        reporter_.Info(current_status_.ToString().c_str());
        return;
      } else if (strict) {
        reporter_.Info("Could seek required sequence number. Iterator will "
                       "continue.");
      }
      is_valid_ = true;
      started_ = true; // set started_ as we could seek till starting sequence
      return;
    } else {
      is_valid_ = false;
    }
  }

  // Could not find start sequence in first file. Normally this must be the
  // only file. Otherwise log the error and let the iterator return next entry
  // If strict is set, we want to seek exactly till the start sequence and it
  // should have been present in the file we scanned above
  if (strict) {
    current_status_ = Status::Corruption(
        "Gap in sequence number. Could not "
        "seek to required sequence number");
    reporter_.Info(current_status_.ToString().c_str());
  } else if (files_->size() != 1) {
    current_status_ = Status::Corruption(
        "Start sequence was not found, "
        "skipping to the next available");
    reporter_.Info(current_status_.ToString().c_str());
    // Let NextImpl find the next available entry. started_ remains false
    // because we don't want to check for gaps while moving to start sequence
    NextImpl(true);
  }
}

void TransactionLogIteratorImpl::Next() {
  return NextImpl(false);
}

void TransactionLogIteratorImpl::NextImpl(bool internal) {
  Slice record;
  is_valid_ = false;
  if (!internal && !started_) {
    // Runs every time until we can seek to the start sequence
    return SeekToStartSequence();
  }
  while(true) {
    assert(current_log_reader_);
    if (current_log_reader_->IsEOF()) {
      current_log_reader_->UnmarkEOF();
    }
    while (RestrictedRead(&record)) { //每次读取一个record，一个record包含一个完整的batch
      if (record.size() < WriteBatchInternal::kHeader) {
        reporter_.Corruption(
          record.size(), Status::Corruption("very small log record"));
        continue;
      } else {
        // started_ should be true if called by application
        assert(internal || started_);
        // started_ should be false if called internally
        assert(!internal || !started_);
        UpdateCurrentWriteBatch(record);  //更新current_batch_seq_、current_batch_（iter->GetBatch返回值内容）
        if (internal && !started_) {
          started_ = true;
        }
        return;
      }
    }

    // Open the next file
    if (current_file_index_ < files_->size() - 1) { //打开下一个wal文件
      ++current_file_index_;
      Status s = OpenLogReader(files_->at(current_file_index_).get());//文件都存在了files_数组中,创建logreader
      if (!s.ok()) {  //打开失败，则会置iter为invalid
        is_valid_ = false;
        current_status_ = s;
        return;
      }
    } else {
      is_valid_ = false;
      if (current_last_seq_ == versions_->LastSequence()) {
        current_status_ = Status::OK();
      } else {
        const char* msg = "Create a new iterator to fetch the new tail.";
        current_status_ = Status::TryAgain(msg);
      }
      return;
    }
  }
}

bool TransactionLogIteratorImpl::IsBatchExpected(
    const WriteBatch* batch, const SequenceNumber expected_seq) {
  assert(batch);
  SequenceNumber batchSeq = WriteBatchInternal::Sequence(batch);
  if (batchSeq != expected_seq) {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "Discontinuity in log records. Got seq=%" PRIu64
             ", Expected seq=%" PRIu64 ", Last flushed seq=%" PRIu64
             ".Log iterator will reseek the correct batch.",
             batchSeq, expected_seq, versions_->LastSequence());
    reporter_.Info(buf);
    return false;
  }
  return true;
}

void TransactionLogIteratorImpl::UpdateCurrentWriteBatch(const Slice& record) {
  std::unique_ptr<WriteBatch> batch(new WriteBatch());
  Status s = WriteBatchInternal::SetContents(batch.get(), record);  //将record的内容放到batch.req_中；所以一个record里是什么内容？此处传入的record就是一个WriteBatch
  s.PermitUncheckedError();  // TODO: What should we do with this error?

  SequenceNumber expected_seq = current_last_seq_ + 1;
  // If the iterator has started, then confirm that we get continuous batches
  if (started_ && !IsBatchExpected(batch.get(), expected_seq)) {  //只有当外部循环中的current_last_seq_ >= starting_sequence_number_时started_才为true
    // Seek to the batch having expected sequence number
    if (expected_seq < files_->at(current_file_index_)->StartSequence()) {
      // Expected batch must lie in the previous log file
      // Avoid underflow.
      if (current_file_index_ != 0) {
        current_file_index_--;
      }
    }
    starting_sequence_number_ = expected_seq;
    // currentStatus_ will be set to Ok if reseek succeeds
    // Note: this is still ok in seq_pre_batch_ && two_write_queuesp_ mode
    // that allows gaps in the WAL since it will still skip over the gap.
    current_status_ = Status::NotFound("Gap in sequence numbers");
    // In seq_per_batch_ mode, gaps in the seq are possible so the strict mode
    // should be disabled
    return SeekToStartSequence(current_file_index_, !seq_per_batch_);
  }

  struct BatchCounter : public WriteBatch::Handler {
    SequenceNumber sequence_;
    BatchCounter(SequenceNumber sequence) : sequence_(sequence) {}
    Status MarkNoop(bool empty_batch) override {
      if (!empty_batch) {
        sequence_++;
      }
      return Status::OK();
    }
    Status MarkEndPrepare(const Slice&) override {
      sequence_++;
      return Status::OK();
    }
    Status MarkCommit(const Slice&) override {
      sequence_++;
      return Status::OK();
    }

    Status PutCF(uint32_t /*cf*/, const Slice& /*key*/,
                 const Slice& /*val*/) override {
      return Status::OK();
    }
    Status DeleteCF(uint32_t /*cf*/, const Slice& /*key*/) override {
      return Status::OK();
    }
    Status SingleDeleteCF(uint32_t /*cf*/, const Slice& /*key*/) override {
      return Status::OK();
    }
    Status MergeCF(uint32_t /*cf*/, const Slice& /*key*/,
                   const Slice& /*val*/) override {
      return Status::OK();
    }
    Status MarkBeginPrepare(bool) override { return Status::OK(); }
    Status MarkRollback(const Slice&) override { return Status::OK(); }
  };

  current_batch_seq_ = WriteBatchInternal::Sequence(batch.get()); //解析出record的sequence作为当前的batchseq
  if (seq_per_batch_) {   //在wal_manager对象wal_manager_创建时候传入，具体又在DBImpl::DBImpl传入功能描述见DBimpl类，默认为false
    BatchCounter counter(current_batch_seq_);
    batch->Iterate(&counter);
    current_last_seq_ = counter.sequence_;
  } else {
    current_last_seq_ =
        current_batch_seq_ + WriteBatchInternal::Count(batch.get()) - 1;  //计算current_batch_seq_
  }
  // currentBatchSeq_ can only change here
  assert(current_last_seq_ <= versions_->LastSequence());

  current_batch_ = std::move(batch);  //current_batch_就是iter->GetBatch()返回的batch内容
  is_valid_ = true;
  current_status_ = Status::OK();
}

Status TransactionLogIteratorImpl::OpenLogReader(const LogFile* log_file) {
  std::unique_ptr<SequentialFileReader> file;
  Status s = OpenLogFile(log_file, &file); //file is filereader,it is a output para
  if (!s.ok()) {
    return s;
  }
  assert(file);
  current_log_reader_.reset(
      new log::Reader(options_->info_log, std::move(file), &reporter_,
                      read_options_.verify_checksums_, log_file->LogNumber()));
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
