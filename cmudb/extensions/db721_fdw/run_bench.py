import argparse
import uuid
import os

def exec_cmd(cmd: str):
    status = os.system(cmd)
    print(f"execute {cmd} with return status {status}")


def run_bench(bench_name: str):
    template = 'hyperfine "/home/melos/db/postgres/build/postgres/bin//psql -U noisepage_user -d noisepage_db -p 15721 -f /home/melos/db/postgres/cmudb/extensions/db721_fdw/{}_benchmark.sql" --export-markdown /home/melos/db/postgres/cmudb/extensions/db721_fdw/bench_{}_{}.md'
    unique_id = uuid.uuid4().hex
    
    final_cmd = template.format(bench_name, bench_name, unique_id)
    exec_cmd(final_cmd)

def init_server():
    exec_cmd('sh /home/melos/db/postgres/cmudb/env/local_postgres.sh')

def init_client():
    exec_cmd('sh /home/melos/db/postgres/cmudb/env/local_psql.sh')

def clear_bench():
    return    

def main():
    parser = argparse.ArgumentParser(
        description="run different benchmark for db721")

    parser.add_argument('-b', type=str, choices=['normal', 'proj_push', 'pred_push', 'complex'],
                        help='the benchmark name to run')

    parser.add_argument('-init', action='store_true')
    args = parser.parse_args()
    
    if args.init:
        init_server()
        init_client()

    run_bench(args.b)


if __name__ == "__main__":
    main()

# hyperfine "/home/melos/db/postgres/build/postgres/bin//psql -U noisepage_user -d noisepage_db -p 15721 -f /home/melos/db/postgres/cmudb/extensions/db721_fdw/naive_benchmark.sql" --export-markdown /home/melos/db/postgres/cmudb/extensions/db721_fdw/res.md
