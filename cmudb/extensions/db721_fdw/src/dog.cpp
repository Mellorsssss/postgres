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

    // metadata["Columns"]["Column Name"] -> column data(json)
    auto &columns = meta_["Columns"];
    for (auto &column : columns.items())
    {
      elog(LOG, "current column is %s", column.key().c_str());

      // TODO: extract the column into other compact datastructure instead of raw json
      column_index_[column.key()] = meta_info_.column_num_;
      meta_info_.column_num_++;

      if (type_map.find(column.value()["type"]) == type_map.end())
      {
        elog(ERROR, "undefined type %s", column.value()["type"]);
      }

      column_info col_info;
      col_info.column_type = type_map[column.value()["type"]];
      col_info.column_name = column.key();
      col_info.start_offset = column.value()["start_offset"];
      col_info.block_size.resize(column.value()["num_blocks"]);
      for (auto& block: column.value()["block_stats"].items()){
        int idx = atoi(block.key().c_str());
        
        if (idx >= static_cast<int>(col_info.block_size.size())){
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
  }

  ReadStatus Db721Reader::ReadColumn(int col_idx)
  {
  }

  ReadStatus Db721Reader::ReadNextBlockBatch()
  {
  }

  /* ReadNextRow */
  ReadStatus Db721Reader::ReadNextRow(TupleTableSlot *slot)
  {
    // TODO: make up a row from the in-memory block data
    return ReadStatus::RS_OK;
  }
}