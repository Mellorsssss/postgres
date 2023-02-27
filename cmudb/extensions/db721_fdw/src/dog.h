#pragma once

// clang-format off
extern "C" {
#include "../../../../src/include/postgres.h"
#include "executor/tuptable.h"
}
// TODO(WAN): Hack.
//  Because PostgreSQL tries to be portable, it makes a bunch of global
//  definitions that can make your C++ libraries very sad.
//  We're just going to undefine those.
#undef vsnprintf
#undef snprintf
#undef vsprintf
#undef sprintf
#undef vfprintf
#undef fprintf
#undef vprintf
#undef printf
#undef gettext
#undef dgettext
#undef ngettext
#undef dngettext
// clang-format on

#include <fcntl.h>
#include <nlohmann/json.hpp>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

using json = nlohmann::json;

class Dog
{
public:
  explicit Dog(std::string name);
  std::string Bark();

private:
  std::string name_;
};

namespace Db721
{

  /**
   * parse module
   * DB721_reader() --> FileReader(support read chunks of data from arbitrary offset)
   */
  class FileReader
  {
  public:
    FileReader(std::string file_name) : filename_(std::move(file_name))
    {
      fd_ = open(filename_.c_str(), O_RDONLY);
      if (fd_ < 0)
      {
        elog(ERROR, "fail to open the file");
      }
    }

    FileReader(const FileReader &) = delete;
    FileReader &operator=(const FileReader &) = delete;

    virtual ~FileReader();

  public:
    virtual void Read(uint64_t offset, size_t n, void *data) const;

    off_t FileSize() const;

  private:
    std::string filename_;
    int fd_{-1};
  };

  enum ReadStatus
  {
    RS_EOF,
    RS_ERR,
    RS_OK,
  };

  class Db721Reader
  {
  private:
    typedef struct
    {
      int column_num_;
      int maxvalue_block_;
      int num_block_; // for this project, all the column have the same number of blocks
      // TODO: num_block_ can be dangerous in the scenario different block have different block value numbers
    } meta_info;

    enum DataType
    {
      INT,     // 4 byte
      FLOAT,   // 4 byte
      VARCHAR, // 32 byte
    };

    static inline std::map<std::string, DataType> type_map = {{"float", DataType::FLOAT}, {"int", DataType::INT}, {"str", DataType::VARCHAR}};

    enum Status
    {
      SUCCESS,
      FAILURE,
    };

    typedef struct
    {
      DataType column_type;
      int start_offset;
      std::string column_name;
      std::vector<int> block_size; // invariant: block_size.size() == meta_info_.num_block_

      void dump()
      {
        elog(LOG, "column: %s, start offset: %d, type: %d, block num: %d", column_name.c_str(), start_offset, static_cast<int>(column_type), static_cast<int>(block_size.size()));
      }
    } column_info;

  public:
    Db721Reader(std::string file_name) : reader_(std::make_unique<FileReader>(file_name))
    {
    }
    Db721Reader(const Db721Reader &) = delete;
    Db721Reader &operator=(const Db721Reader &) = delete;
    ~Db721Reader() = default;

  public:
    void Init();

    void Debug() const;

  private:
    /* ReadColumn fetch all column data */
    ReadStatus ReadColumn(int col_idx);

    /* ReadNextBlock fetch all the blocks into memory according to the schema*/
    ReadStatus ReadNextBlockBatch();

    /* ReadNextRow */
    ReadStatus ReadNextRow(TupleTableSlot *slot);

    Status GetColumnOffset(int col_idx, int block_idx);

  private:
    std::unique_ptr<FileReader> reader_;
    std::vector<void *> column_data_; // current batch of column data

    /* meta data */
    json meta_;
    meta_info meta_info_{.column_num_ = 0, .maxvalue_block_ = 0, .num_block_ = 0};

    /* map column name to column index */
    std::map<std::string, int> column_index_;

    std::vector<column_info> column_type_;

    /* read state */
    int total_row_{0}; // total row number of current in-memory(column_data_) block
    int row_{0};       // invariant: row_  <= total_row_
  };
}