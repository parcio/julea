from kv.kv import benchmark_kv
from object.object import benchmark_object
from object.distributed_object import benchmark_distributed_object
from db.entry import benchmark_db_entry
from db.iterator import benchmark_db_iterator
from db.schema import benchmark_db_schema
from item.collection import benchmark_collection
from item.item import benchmark_item
from benchmarkrun import print_header
from sys import argv

if __name__ == "__main__":
    runs = []
    iterations = 1000
    machine_readable = ("-m" in argv)
    print_header(machine_readable)

    # KV Client
    benchmark_kv(runs, iterations, machine_readable)

    # Object Client
    benchmark_distributed_object(runs, iterations, machine_readable)
    benchmark_object(runs, iterations, machine_readable)

    # DB Client
    benchmark_db_entry(runs, iterations, machine_readable)
    benchmark_db_iterator(runs, iterations, machine_readable)
    benchmark_db_schema(runs, iterations, machine_readable)

    # Item Client
    benchmark_collection(runs, iterations, machine_readable)
    benchmark_item(runs, iterations, machine_readable)
