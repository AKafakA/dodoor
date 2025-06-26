from deploy.python.data.generator.data_generator import DataGenerator
import math
from abc import ABC
import pandas as pd
import random


MIN_CORES = 1


class ServerlessDataGenerator(DataGenerator, ABC):
    def __init__(self,
                 data_path):
        super().__init__()
        self.data_path = data_path

    def generate(self,
                 num_records,
                 start_id, max_duration=-1,
                 time_range_in_days=None,
                 selected_task_ids=range(1, 28),
                 max_cores=7, max_memory=61 * 1024):

        time_range_in_seconds = []
        data = []
        if time_range_in_days is None:
            time_range_in_days = [0, 0.1]
        for i in range(len(time_range_in_days)):
            time_range_in_seconds.append(time_range_in_days[i] * 24 * 60 * 60)

        request_trace = self.data_path + "/request.csv"
        function_trace = self.data_path + "/function.csv"
        cpu_trace = self.data_path + "/cpu.csv"
        memory_trace = self.data_path + "/memory.csv"

        request_trace = pd.read_csv(request_trace)
        function_trace = pd.read_csv(function_trace)
        cpu_trace = pd.read_csv(cpu_trace)
        memory_trace = pd.read_csv(memory_trace)

        for function_id  in selected_task_ids:
            request_trace[str(function_id)] = request_trace[str(function_id)].fillna(0)
            function_trace[str(function_id)] = function_trace[str(function_id)].fillna(0)
            cpu_trace[str(function_id)] = cpu_trace[str(function_id)].fillna(-1)
            memory_trace[str(function_id)] = memory_trace[str(function_id)].fillna(-1)

        memories = {}
        cpus = {}
        for function_id in selected_task_ids:
            for i in range(len(memory_trace)):
                if memory_trace.loc[i, str(function_id)] != -1 and cpu_trace.loc[i, str(function_id)] != -1:
                    memories[str(function_id)] = int(min(memory_trace.loc[i, str(function_id)], max_memory))
                    cpus[str(function_id)] = int(min(max(math.ceil(cpu_trace.loc[i, str(function_id)]), MIN_CORES),
                                                     max_cores))
                    break

        print("Memory: {}".format(memories))
        print("CPU: {}".format(cpus))

        accumulated_num_records = 0
        for seconds in range(len(request_trace)):
            if seconds < time_range_in_seconds[0]:
                continue
            if 0 < num_records <= accumulated_num_records or seconds > time_range_in_seconds[1]:
                break
            time = request_trace.loc[seconds, "time"] * 1000
            for function_id in selected_task_ids:
                new_request = request_trace.loc[seconds, str(function_id)]
                duration = function_trace.loc[seconds, str(function_id)]
                duration_nano = duration * 1000000
                for k in range(int(new_request)):
                    accumulated_num_records += 1
                    data.append({
                        "taskId": accumulated_num_records,
                        # "cores": int(min(max(math.ceil(cpu), MIN_CORES), max_cores)),
                        "cores": cpus[str(function_id)],
                        # "memory": int(min(memory, max_memory)),
                        "memory": memories[str(function_id)],
                        "disk": 0,
                        "duration": int(duration_nano),
                        "startTime": int(time + random.randint(0, 999))
                    })

        sorted_data = sorted(data, key=lambda x: x["startTime"])
        for i in range(len(sorted_data)):
            sorted_data[i]["taskId"] = i + start_id

        print("Accumulated num records: {}".format(accumulated_num_records))
        return sorted_data


