// If you choose to use C++, read this very carefully:
// https://www.postgresql.org/docs/15/xfunc-c.html#EXTEND-CPP

#include "dog.h"
#include <iostream>

// clang-format off
extern "C" {
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"
#include "../../../../src/include/foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "commands/defrem.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
}
// clang-format on

namespace Db721_utils
{
  typedef enum
  {
    PS_START = 0,
    PS_IDENT,
    PS_QUOTE
  } ParserState;

  static List *
  parse_filenames_list(const char *str)
  {
    char *cur = pstrdup(str);
    char *f = cur;
    ParserState state = PS_START;
    List *filenames = NIL;

    while (*cur)
    {
      switch (state)
      {
      case PS_START:
        switch (*cur)
        {
        case ' ':
          /* just skip */
          break;
        case '"':
          f = cur + 1;
          state = PS_QUOTE;
          break;
        default:
          /* XXX we should check that *cur is a valid path symbol
           * but let's skip it for now */
          state = PS_IDENT;
          f = cur;
          break;
        }
        break;
      case PS_IDENT:
        switch (*cur)
        {
        case ' ':
          *cur = '\0';
          filenames = lappend(filenames, makeString(f));
          state = PS_START;
          break;
        default:
          break;
        }
        break;
      case PS_QUOTE:
        switch (*cur)
        {
        case '"':
          *cur = '\0';
          filenames = lappend(filenames, makeString(f));
          state = PS_START;
          break;
        default:
          break;
        }
        break;
      default:
        elog(ERROR, "parquet_fdw: unknown parse state");
      }
      cur++;
    }
    filenames = lappend(filenames, makeString(f));

    return filenames;
  }

  // modified from parquet_impl.cpp/get_table_options
  static void init_fdwplan_state(Oid foreigntableid, Db721::Db721FdwPlanState *fdw_private)
  {
    ForeignTable *table;
    ListCell *lc;
    char *funcname = NULL;
    char *funcarg = NULL;

    fdw_private->use_mmap = false;
    fdw_private->use_threads = false;
    fdw_private->max_open_files = 0;
    fdw_private->files_in_order = false;
    table = GetForeignTable(foreigntableid);

    foreach (lc, table->options)
    {
      DefElem *def = (DefElem *)lfirst(lc);

      if (strcmp(def->defname, "filename") == 0)
      {
        elog(LOG, "parse the filename");
        fdw_private->filenames = parse_filenames_list(defGetString(def));
      }
      else if (strcmp(def->defname, "files_func") == 0)
      {
        elog(LOG, "parse the files_func");
        funcname = defGetString(def);
      }
      else if (strcmp(def->defname, "files_func_arg") == 0)
      {
        funcarg = defGetString(def);
      }
      else if (strcmp(def->defname, "sorted") == 0)
      {
        elog(LOG, "parse the sorted");
        elog(LOG, "todo: handle attr list");
        // fdw_private->attrs_sorted =
        // parse_attributes_list(defGetString(def), foreigntableid);
      }
      else if (strcmp(def->defname, "use_mmap") == 0)
      {
        elog(LOG, "parse the use_mmap");
        fdw_private->use_mmap = defGetBoolean(def);
      }
      else if (strcmp(def->defname, "use_threads") == 0)
      {
        elog(LOG, "parse the use_threads");
        fdw_private->use_threads = defGetBoolean(def);
      }
      else if (strcmp(def->defname, "max_open_files") == 0)
      {
        elog(LOG, "parse the max_open_files");
        /* check that int value is valid */
        fdw_private->max_open_files = atoi(defGetString(def));
      }
      else if (strcmp(def->defname, "files_in_order") == 0)
      {
        elog(LOG, "parse the files_in_order");
        fdw_private->files_in_order = defGetBoolean(def);
      }
      else if (strcmp(def->defname, "tablename") == 0)
      {
        elog(LOG, "parse the tablename:%d", def->type);
        // fdw_private->table_name = std::string(defGetString(def));
      }
      else
        elog(LOG, "unknown option '%s'", def->defname);
    }

    if (funcname)
    {
      elog(LOG, "todo: handle the funcname");
      // fdw_private->filenames = get_filenames_from_userfunc(funcname, funcarg);
    }
  }
}

extern "C" void db721_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                        Oid foreigntableid)
{
  elog(LOG, "db721_GetForeignRelSize\n");

  Db721::Db721FdwPlanState *fdwplanstate = (Db721::Db721FdwPlanState *)palloc(sizeof(Db721::Db721FdwPlanState));
  Db721_utils::init_fdwplan_state(foreigntableid, fdwplanstate);

  // there should only be one .db721 file
  std::string filename;
  ListCell *lc;
  foreach (lc, fdwplanstate->filenames)
  {
    filename = std::string(strVal(lfirst(lc)));
  }

  // std::unique_ptr<Db721::Db721Reader> db721reader = std::make_unique<Db721::Db721Reader>(filename, CurrentMemoryContext);
  // db721reader->Init();

  // TODO: we only return the total row number
  // baserel->rows = static_cast<double>(db721reader->RowNumber());
  // baserel->tuples = static_cast<double>(db721reader->RowNumber());
  baserel->fdw_private = static_cast<void *>(fdwplanstate);
}

extern "C" void db721_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
                                      Oid foreigntableid)
{
  elog(LOG, "db721_GetForeignPaths\n");

  Cost dummy_cost = 0.0;
  auto foreign_path = (Path *)create_foreignscan_path(root, baserel,
                                                      NULL, /* default pathtarget */
                                                      baserel->rows,
                                                      dummy_cost,
                                                      dummy_cost,
                                                      NULL, /* no pathkeys */
                                                      NULL, /* no outer rel either */
                                                      NULL, /* no extra plan */
                                                      (List *)(baserel->fdw_private));

  add_path(baserel, foreign_path);
}

extern "C" ForeignScan *
db721_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
                     ForeignPath *best_path, List *tlist, List *scan_clauses,
                     Plan *outer_plan)
{
  elog(LOG, "db721_GetForeignPlan\n");
  Db721::Db721FdwPlanState *fdw_private = reinterpret_cast<Db721::Db721FdwPlanState *>(best_path->fdw_private);
  Index scan_relid = baserel->relid;
  List *params = NIL;

  params = lappend(params, fdw_private->filenames);
  params = lappend(params, makeInteger(fdw_private->use_mmap));
  params = lappend(params, makeInteger(fdw_private->use_threads));
  params = lappend(params, makeInteger(fdw_private->max_open_files));
  return make_foreignscan(tlist,
                          scan_clauses,
                          scan_relid,
                          NIL, /* no expressions to evaluate */
                          params,
                          NIL, /* no custom tlist */
                          NIL, /* no remote quals */
                          outer_plan);
}

void destroy_db721_estate(void *fdwstate)
{
  Db721::Db721ExecutionState *estate = reinterpret_cast<Db721::Db721ExecutionState *>(fdwstate);
  if (estate)
  {
    delete estate;
  }
}

extern "C" void db721_BeginForeignScan(ForeignScanState *node, int eflags)
{
  elog(LOG, "db721_BeginForeignScan");

  ForeignScan *plan = reinterpret_cast<ForeignScan *>(node->ss.ps.plan);
  List *fdw_private = plan->fdw_private;
  ListCell *lc;
  List *filenames = NIL;

  int i = 0;
  foreach (lc, fdw_private)
  {
    switch (i)
    {
    case 0:
      filenames = (List *)lfirst(lc);
      break;
    default:
      elog(LOG, "not supported now for %d-th option", i);
    }
    ++i;
  }

  // get the file path of .db721
  // asuumption: there should only be one .db721 file
  std::string filename;
  foreach (lc, filenames)
  {
    filename = std::string(strVal(lfirst(lc)));
  }

  // allocate a long-lived MemoryContext for the Db721Reader
  auto estate = node->ss.ps.state;
  MemoryContext ctx = estate->es_query_cxt;
  MemoryContext reader_ctx = AllocSetContextCreateInternal(ctx, "db721 reader", ALLOCSET_DEFAULT_SIZES);

  Db721::Db721ExecutionState *fdwestate = Db721::CreateDb721ExecutionState(filename, reader_ctx);
  node->fdw_state = reinterpret_cast<void *>(fdwestate);

  MemoryContextCallback *callback = (MemoryContextCallback *)palloc(sizeof(MemoryContextCallback));
  callback->func = destroy_db721_estate;
  callback->arg = (void *)fdwestate;
  MemoryContextRegisterResetCallback(reader_ctx, callback);
}

extern "C" TupleTableSlot *db721_IterateForeignScan(ForeignScanState *node)
{
  Db721::Db721ExecutionState *estate = reinterpret_cast<Db721::Db721ExecutionState *>(node->fdw_state);
  auto &slot = node->ss.ss_ScanTupleSlot;
  ExecClearTuple(slot);
  estate->Next(slot);
  return slot;
}

extern "C" void db721_ReScanForeignScan(ForeignScanState *node)
{
  elog(LOG, "db721_ReScanForeignScan");
  Db721::Db721ExecutionState *estate = reinterpret_cast<Db721::Db721ExecutionState *>(node->fdw_state);
  estate->Reset();
}

extern "C" void db721_EndForeignScan(ForeignScanState *node)
{
  elog(LOG, "db721_EndForeignScan");
}