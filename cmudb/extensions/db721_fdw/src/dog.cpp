#include "dog.h"
#include <iostream>

Dog::Dog(std::string name) : name_(name) {}

std::string Dog::Bark() { return name_ + ": Woof!"; }

namespace Db721
{
  /* FileReader */
  FileReader::~FileReader() = default;

  void FileReader::Read(uint64_t offset, size_t n, void *data) const
  {
    ssize_t read_size = pread(fd_, data, n, static_cast<off_t>(offset));

    if (read_size < 0)
    {
      elog(ERROR, "fail to read the file");
    }
  }

  off_t FileReader::FileSize() const
  {
    struct stat st;
    stat(filename_.c_str(), &st);

    elog(LOG, "the size of the file is %ld", st.st_size);
    return st.st_size;
  }
}

namespace Db721
{
  /* Db721Reader */
  void Db721Reader::Debug() const
  {
    int file_size = reader_->FileSize();
    int meta_size = 0;
    reader_->Read(file_size - 4, 4, &meta_size);
    elog(LOG, "the meta size of the file is %d", meta_size);

    char *buf = static_cast<char *>(palloc0(meta_size + 1));

    reader_->Read(file_size - 4 - meta_size, meta_size, static_cast<void *>(buf));
    buf[meta_size] = '\0';

    json meta = json::parse(buf);
    std::cout << meta << std::endl;

    pfree(static_cast<void *>(buf));
  }

  void Db721Reader::Init()
  {
    // read the meta and deserailize it into json
    int file_size = reader_->FileSize();
    int meta_size = 0;
    reader_->Read(file_size - 4, 4, &meta_size);
    elog(LOG, "the meta size of the file is %d", meta_size);

    char *buf = static_cast<char *>(palloc0(meta_size + 1));

    reader_->Read(file_size - 4 - meta_size, meta_size, static_cast<void *>(buf));
    buf[meta_size] = '\0';

    meta_ = std::move(json::parse(buf));

    pfree(static_cast<void *>(buf));

    std::cout << meta_ << std::endl;

    // maintain the basic information
    meta_info_.maxvalue_block_ = meta_["Max Values Per Block"];
    meta_info_.table_name_ = std::move(meta_["Table"]);

    // metadata["Columns"]["Column Name"] -> column data(json)
    auto &columns = meta_["Columns"];
    for (auto &column : columns.items())
    {
      elog(LOG, "current column is %s", column.key().c_str());

      // TODO: extract the column into other compact datastructure instead of raw json
      column_index_[column.key()] = meta_info_.column_num_;
      meta_info_.column_num_++;

      if (type_map.find(column.value()["type"]) == type_map.end() || type_size.find(column.value()["type"]) == type_size.end())
      {
        elog(ERROR, "undefined type %s", column.value()["type"]);
      }

      column_info col_info;
      col_info.column_type = type_map[column.value()["type"]];
      col_info.type_size = type_size[column.value()["type"]];
      col_info.column_name = column.key();
      col_info.start_offset = column.value()["start_offset"];
      col_info.block_size.resize(column.value()["num_blocks"]);

      for (auto &block : column.value()["block_stats"].items())
      {
        int idx = atoi(block.key().c_str());

        if (idx >= static_cast<int>(col_info.block_size.size()))
        {
          elog(ERROR, "invalid block index %d", idx);
        }
        col_info.block_size[idx] = block.value()["num"];
      }

      col_info.dump();
      column_type_.emplace_back(col_info);

      if (meta_info_.num_block_ == 0)
      {
        meta_info_.num_block_ = column.value()["num_blocks"];
      }
    }

    meta_info_.dump();
    Reset();
    elog(LOG, "Db721 of init success");
  }

  inline ReadStatus Db721Reader::ReadColumn(int col_idx)
  {
    if (cur_block_idx_ >= meta_info_.num_block_)
    {
      elog(ERROR, "fail to read block %d", cur_block_idx_);
    }

    int offset;
    Status s = GetColumnOffset(col_idx, cur_block_idx_, offset);
    if (s != Status::SUCCESS)
    {
      elog(ERROR, "fail to get the column(%d, %d) offset", col_idx, cur_block_idx_);
    }

    if (column_data_[col_idx] == nullptr)
    {
      // allocate the size of maximun possible chunk size
      column_data_[col_idx] = palloc0(meta_info_.maxvalue_block_ * column_type_[col_idx].type_size);
    }

    int read_size = column_type_[col_idx].block_size[cur_block_idx_] * column_type_[col_idx].type_size;
    reader_->Read(offset, read_size, column_data_[col_idx]);

    elog(LOG, "read column %d at %d, total %d bytes", col_idx, offset, read_size);
    return ReadStatus::RS_OK;
  }

  inline ReadStatus Db721Reader::ReadNextBlockBatch()
  {
    if (cur_block_idx_ == meta_info_.num_block_)
    {
      return ReadStatus::RS_EOF;
    }

    for (int col_idx = 0; col_idx < meta_info_.column_num_; col_idx++)
    {
      if (ReadStatus::RS_OK != ReadColumn(col_idx))
      {
        elog(LOG, "fail to read column %d", col_idx);
        return ReadStatus::RS_ERR;
      }
    }

    total_row_ = column_type_[0].block_size[cur_block_idx_]; // hack: assume all the block with same idx hold the same number of values
    row_ = 0;
    cur_block_idx_++;

    return ReadStatus::RS_OK;
  }

  /* ReadNextRow */
  ReadStatus Db721Reader::ReadNextRow(TupleTableSlot *slot)
  {
    // TODO: make up a row from the in-memory block data
    if (row_ >= total_row_)
    {
      ReadStatus s;
      if (ReadStatus::RS_OK != (s = ReadNextBlockBatch()))
      {
        if (s != ReadStatus::RS_EOF)
        {
          elog(LOG, "fail to read the next block batch");
        }
        else
        {
          elog(LOG, "no more rows");
        }
        return s;
      }
    }

    // TODO: put all the rows into the slot(order?)

    // simulate the row
    std::cout << row_ << "| ";
    int vi;
    float vf;
    char *vc;
    for (int col_idx = 0; col_idx < meta_info_.column_num_; ++col_idx)
    {
      switch (column_type_[col_idx].column_type)
      {
      case DataType::INT:
        vi = ((int *)(column_data_[col_idx]))[row_];
        std::cout << vi << " | ";
        break;
      case DataType::FLOAT:
        vf = ((float *)(column_data_[col_idx]))[row_];
        std::cout << vf << " | ";
        break;
      case DataType::VARCHAR:
        vc = ((char *)(column_data_[col_idx])) + 32 * row_;
        std::cout << vc << " | ";
        break;
      default:
        elog(ERROR, "unexpected datatype");
      }
    }
    std::cout << "\n";
    ++row_;

    return ReadStatus::RS_OK;
  }

  Db721Reader::Status Db721Reader::GetColumnOffset(int col_idx, int block_idx, int &offset)
  {
    if (col_idx >= meta_info_.column_num_)
    {
      return Status::FAILURE;
    }

    // hack: we assume all the block has the block size as the maxvalue_block except last one
    offset = block_idx * meta_info_.maxvalue_block_ * column_type_[col_idx].type_size + column_type_[col_idx].start_offset;
    return Status::SUCCESS;
  }
}