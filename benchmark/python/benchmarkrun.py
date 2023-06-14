from time import perf_counter_ns

class BenchmarkRun:
    def __init__(self, name, iterations, machine_readable):
        self.name = name
        self.iterations = iterations
        self.timer_started = False
        self.start = None
        self.stop = None
        self.break_start = None
        self.total_break = 0
        self.operations = iterations
        self.machine_readable = machine_readable
        self.iteration_count = 0
        self.i = 0
        self.op_duration = 1000 ** 3
        self.batch_send = None
        self.batch_clean = None

    def __iter__(self):
        self.start_timer()
        return self

    def __next__(self):
        self.i += 1
        if self.i >= self.operations:
            self.i = 0
            self.iteration_count += 1
            if self.batch_send is not None:
                if not self.batch_send():
                    raise RuntimeError("Failed to execute batch!")
            if self.batch_clean is not None:
                self.pause_timer()
                if not self.batch_clean():
                    raise RuntimeError("Failed to clean batch!")
                self.continue_timer()
            if perf_counter_ns() - self.start > self.op_duration:
                self.stop_timer()
                raise StopIteration
        return self.i

    def pause_timer(self):
        self.break_start = perf_counter_ns()

    def continue_timer(self):
        self.total_break += perf_counter_ns() - self.break_start
        self.break_start = None

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
            return self.stop - self.start - self.total_break

    def print_result(self):
        if self.machine_readable:
            print(f"{self.name},{self.get_runtime_s()},{self.operations}")
        else:
            name_col = self.name.ljust(60," ")
            runtime_col = f"{self.get_runtime_s():.3f}".rjust(8," ") + " seconds"
            operations_col = f"{int(float(self.operations)*self.iteration_count/self.get_runtime_s())}/s".rjust(12," ")
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
    # try:
    func(run)
    run.print_result()
    # except:
        # run.print_empty()

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
