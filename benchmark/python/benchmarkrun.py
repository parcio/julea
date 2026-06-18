from time import perf_counter_ns

class BenchmarkRun:
    def __init__(self, name, iterations, machine_readable):
        self.name = name
        self.iterations = iterations
        self.timer_started = False
        self.start = None
        self.stop = None
        self.operations = iterations
        self.machine_readable = machine_readable

    def start_timer(self):
        self.timer_started = True
        self.start = perf_counter_ns()

    def stop_timer(self):
        self.timer_started = False
        self.stop = perf_counter_ns()

    def get_runtime_ms(self):
        val = self.get_runtime_ns()
        return val / 1000000 if val != None else None

    def get_runtime_s(self):
        val = self.get_runtime_ns()
        return val / 1000000000 if val != None else None

    def get_runtime_ns(self):
        if self.timer_started or self.stop == None:
            return None
        else:
            return self.stop - self.start

    def print_result(self):
        if self.machine_readable:
            print(f"{self.name},{self.get_runtime_s()},{self.operations}")
        else:
            name_col = self.name.ljust(60," ")
            runtime_col = f"{self.get_runtime_s():.3f}".rjust(8," ") + " seconds"
            operations_col = f"{int(self.operations/self.get_runtime_s())}/s".rjust(12," ")
            print(f"{name_col} | {runtime_col} | {operations_col}")

    def print_empty(self):
        if self.machine_readable:
            print(f"{self.name},-,-")
        else:
            name_col = self.name.ljust(60," ")
            runtime_col = "-".rjust(8," ") + " seconds"
            operations_col = "-/s".rjust(12," ")
            print(f"{name_col} | {runtime_col} | {operations_col}")

def append_to_benchmark_list_and_run(_list, run, func):
    _list.append(run)
    try:
        func(run)
        run.print_result()
    except:
        run.print_empty()

def print_result_table_header():
    name_col = "Name".ljust(60," ")
    runtime_col = "Duration".ljust(16," ")
    operations_col = f"Operations/s".rjust(12," ")
    header = f"{name_col} | {runtime_col} | {operations_col}"
    print(header+"\n"+len(header)*"-")

def print_machine_readable_header():
    print("name,elapsed,operations")

def print_header(machine_readable):
    if machine_readable:
        print_machine_readable_header()
    else:
        print_result_table_header()
