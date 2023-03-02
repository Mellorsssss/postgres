#pragma once

// clang-format off
extern "C" {
#include "../../../../src/include/postgres.h"
#include "executor/tuptable.h"
#include "utils/memutils.h"
#include "utils/memdebug.h"
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
#define SEGMENT_SIZE (1024 * 1024)

#include <fcntl.h>
#include <list>
#include <nlohmann/json.hpp>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
#include <set>

using json = nlohmann::ordered_json;

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

  struct Db721FdwPlanState
  {
    List *filenames;
    List *attrs_sorted;
    Bitmapset *attrs_used; /* attributes actually used in query */
    bool use_mmap;
    bool use_threads;
    int32 max_open_files;
    bool files_in_order;
    std::string table_name;
    // List *rowgroups; /* List of Lists (per filename) */
    // uint64 matched_rows;
  };

  /*
   * exc_palloc
   *      C++ specific memory allocator that utilizes postgres allocation sets.
   */
  void *
  exc_palloc(std::size_t size);

  class FastAllocator
  {
  private:
    /*
     * Special memory segment to speed up bytea/Text allocations.
     */
    MemoryContext segments_cxt;
    char *segment_start_ptr;
    char *segment_cur_ptr;
    char *segment_last_ptr;
    std::list<char *> garbage_segments;

  public:
    FastAllocator(MemoryContext cxt)
        : segments_cxt(cxt), segment_start_ptr(nullptr), segment_cur_ptr(nullptr),
          segment_last_ptr(nullptr), garbage_segments()
    {
    }

    ~FastAllocator()
    {
      this->recycle();
    }

    /*
     * fast_alloc
     *      Preallocate a big memory segment and distribute blocks from it. When
     *      segment is exhausted it is added to garbage_segments list and freed
     *      on the next executor's iteration. If requested size is bigger that
     *      SEGMENT_SIZE then just palloc is used.
     */
    inline void *fast_alloc(long size)
    {
      void *ret;

      Assert(size >= 0);

      /* If allocation is bigger than segment then just palloc */
      if (size > SEGMENT_SIZE)
      {
        MemoryContext oldcxt = MemoryContextSwitchTo(this->segments_cxt);
        void *block = exc_palloc(size);
        this->garbage_segments.push_back((char *)block);
        MemoryContextSwitchTo(oldcxt);

        return block;
      }

      size = MAXALIGN(size);

      /* If there is not enough space in current segment create a new one */
      if (this->segment_last_ptr - this->segment_cur_ptr < size)
      {
        MemoryContext oldcxt;

        /*
         * Recycle the last segment at the next iteration (if there
         * was one)
         */
        if (this->segment_start_ptr)
          this->garbage_segments.push_back(this->segment_start_ptr);

        oldcxt = MemoryContextSwitchTo(this->segments_cxt);
        this->segment_start_ptr = (char *)exc_palloc(SEGMENT_SIZE);
        this->segment_cur_ptr = this->segment_start_ptr;
        this->segment_last_ptr =
            this->segment_start_ptr + SEGMENT_SIZE - 1;
        MemoryContextSwitchTo(oldcxt);
      }

      ret = (void *)this->segment_cur_ptr;
      this->segment_cur_ptr += size;

      return ret;
    }

    void recycle(void)
    {
      /* recycle old segments if any */
      if (!this->garbage_segments.empty())
      {
        bool error = false;

        PG_TRY();
        {
          for (auto it : this->garbage_segments)
          {
            pfree(it);
          }
        }
        PG_CATCH();
        {
          error = true;
        }
        PG_END_TRY();
        if (error)
        {
          elog(ERROR, "fail to recycle the files");
          throw std::runtime_error("garbage segments recycle failed");
        }

        this->garbage_segments.clear();
        elog(DEBUG1, "parquet_fdw: garbage segments recycled");
      }
    }

    MemoryContext context()
    {
      return segments_cxt;
    }
  };

  class Db721Reader
  {
  private:
    typedef struct
    {
      int column_num_;
      int maxvalue_block_;
      int num_block_; // for this project, all the column have the same number of blocks
      std::string table_name_;
      // TODO: num_block_ can be dangerous in the scenario different block have different block value numbers

      void dump()
      {
        elog(LOG, "table %s's meta data: %d columns, maxvalue per block is %d, has %d blocks in total", table_name_.c_str(), column_num_, maxvalue_block_, num_block_);
      }
    } meta_info;

    enum DataType
    {
      INT,     // 4 byte
      FLOAT,   // 4 byte
      VARCHAR, // 32 byte
    };

    static inline std::map<std::string, DataType> type_map = {{"float", DataType::FLOAT}, {"int", DataType::INT}, {"str", DataType::VARCHAR}};
    static inline std::map<std::string, int> type_size = {{"float", 4}, {"int", 4}, {"str", 32}};

    enum Status
    {
      SUCCESS,
      FAILURE,
    };

    typedef struct
    {
      DataType column_type;
      int start_offset;
      int type_size;
      std::string column_name;
      std::vector<int> block_size; // invariant: block_size.size() == meta_info_.num_block_

      void dump()
      {
        elog(LOG, "column: %s, start offset: %d, type: %d(%d bytes), block num: %d", column_name.c_str(), start_offset, static_cast<int>(column_type), type_size, static_cast<int>(block_size.size()));

        for (size_t ind = 0; ind < block_size.size(); ++ind)
        {
          elog(LOG, "column: %s block %ld-th's size is %d", column_name.c_str(), ind, block_size[ind]);
        }
      }
    } column_info;

  public:
    Db721Reader(std::string file_name, MemoryContext ctx, std::set<int> attrs) : reader_(std::make_unique<FileReader>(file_name)), allocator_(std::make_unique<FastAllocator>(ctx)), attrs_(std::move(attrs))
    {
    }
    Db721Reader(const Db721Reader &) = delete;
    Db721Reader &operator=(const Db721Reader &) = delete;
    ~Db721Reader() = default;

  public:
    void Init();

    void Reset()
    {
      total_row_ = 0;
      row_ = 0;
      cur_block_idx_ = 0;
      // TODO: consider memory safety
      // column_data_.resize(meta_info_.column_num_, nullptr);
    }

    void Debug() const;

    /* ReadNextRow */
    ReadStatus ReadNextRow(TupleTableSlot *slot);

    int RowNumber() const
    {
      // TODO: this is just an approximation
      return meta_info_.num_block_ * column_type_[0].block_size[0];
    }

  private:
    /* ReadColumn fetch all column data */
    ReadStatus ReadColumn(int col_idx);

    /* ReadNextBlock fetch all the blocks into memory according to the schema*/
    ReadStatus ReadNextBlockBatch();

    Status GetColumnOffset(int col_idx, int block_idx, int &offset);

  private:
    std::unique_ptr<FileReader> reader_;
    std::unique_ptr<FastAllocator> allocator_;

    /* current batch of column data, duckdb treat every data as DataChunk
       for simplicity, just use void* here */
    std::vector<void *> column_data_;

    /* meta data */
    json meta_; // cache this, maybe we should drop it?
    meta_info meta_info_{.column_num_ = 0, .maxvalue_block_ = 0, .num_block_ = 0};

    /* map column name to column index */
    std::map<std::string, int> column_index_;

    std::vector<int> attr_used_;
    std::vector<column_info> column_type_;
    std::set<int> attrs_; // attributes used

    /* read state */
    int total_row_{0}; // total row number of current in-memory(column_data_) block
    int row_{0};       // invariant: row_  <= total_row_
    int cur_block_idx_{0};
  };

  class Db721ExecutionState
  {
  public:
    Db721ExecutionState() = default;
    Db721ExecutionState(const Db721ExecutionState &) = delete;
    Db721ExecutionState &operator=(const Db721ExecutionState &) = delete;
    virtual ~Db721ExecutionState() = default;

  public:
    virtual void Next(TupleTableSlot *slot) = 0;
    virtual void Reset() = 0;
  };

  class NaiveExecutionState final : public Db721ExecutionState
  {
  public:
    NaiveExecutionState(std::string filename, MemoryContext ctx, std::set<int> attrs) : reader_(std::make_unique<Db721Reader>(filename, ctx, attrs))
    {
      reader_->Init();
    }

    NaiveExecutionState() = default;
    ~NaiveExecutionState()
    {
      elog(LOG, "NaiveExecution state is destructed");
    };

  public:
    void Next(TupleTableSlot *slot) override
    {
      if (ReadStatus::RS_OK == reader_->ReadNextRow(slot))
      {
        ExecStoreVirtualTuple(slot);
      }
    }

    void Reset()
    {
      reader_->Reset();
    }

  private:
    std::unique_ptr<Db721Reader> reader_;
  };

  Db721ExecutionState *CreateDb721ExecutionState(const std::string &filename, MemoryContext ctx, std::set<int> attrs);
}