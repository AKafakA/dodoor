from abc import ABC
import numpy as np
import csv
from deploy.python.data.generator.data_generator import DataGenerator
from deploy.python.data.generator.utils import get_wait_time


class ServerlessDataGenerator(DataGenerator, ABC):
    def __init__(self,
                 serverless_dat_dir,
                 target_cluster_qps,
                 distribution_type="gamma",
                 burstiness=1.0,
                 num_functions=200):
        super().__init__()
        self._serverless_dat_dir = serverless_dat_dir
        self._distribution_type = distribution_type
        self._burstiness = burstiness
        self._num_functions = num_functions

        # Load data from CSV files
        self._requests = self._load_csv_data(f"{serverless_dat_dir}/request_call.csv", int)
        self._cpu_cores = self._load_csv_data(f"{serverless_dat_dir}/cpu.csv", float)
        self._memory = self._load_csv_data(f"{serverless_dat_dir}/memory.csv", float)
        self._delay = self._load_csv_data(f"{serverless_dat_dir}/request_delay.csv", float)

        # Ensure all data files have the same number of rows
        assert len(self._requests) == len(self._cpu_cores) == len(self._memory) == len(self._delay), \
            "Mismatch in the number of rows in the input files."

        self._target_qps = target_cluster_qps

    def _load_csv_data(self, file_path, data_type):
        """Helper function to load and parse data from a CSV file."""
        data = []
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            # Assuming the first row is a header, skip it.
            # If there's no header, you can remove next(reader, None).
            next(reader, None)
            for row in reader:
                # We expect columns for day, time, and then functions
                # Let's slice from the 2nd column onwards for function data
                # Handle empty strings by defaulting to 0
                values = [(data_type(val) if val else 0) for val in row[2:]]

                # Ensure the row has the expected number of function columns
                if len(values) < self._num_functions:
                    values.extend([data_type(0)] * (self._num_functions - len(values)))

                data.append(values[:self._num_functions])
        return data

    def generate(self, num_records, start_id, max_duration=-1, time_range_in_days=None):
        if time_range_in_days is None:
            time_range_in_days = [0, 1]
        elif 0 <= time_range_in_days[0] < time_range_in_days[1] <= 1:
            raise ValueError("Time range must be between 0 and 1, representing days.")

        # We only have 1 day of data in the source files (1440 minutes)
        # Let's cap the simulation time to what's available.
        start_minute = int(time_range_in_days[0] * 24 * 60)
        end_minute = int(time_range_in_days[1] * 24 * 60)
        total_duration_in_minutes = end_minute - start_minute

        task_id = start_id
        start_time_ms = start_minute * 60 * 1000

        generated_tasks = []
        cpu_cores_list = []
        memory_list = []

        for minute_offset in range(total_duration_in_minutes):
            current_minute_index = start_minute + minute_offset

            requests_per_function = self._requests[current_minute_index]
            total_requests_in_minute = sum(requests_per_function)

            # If there are no requests in this minute, skip to the next
            if total_requests_in_minute == 0:
                continue

            # Calculate the probability distribution for function selection
            probabilities = np.array(requests_per_function) / total_requests_in_minute

            # Determine how many tasks to generate in this minute based on QPS
            num_tasks_this_minute = int(self._target_qps * 60)

            for _ in range(num_tasks_this_minute):
                # Randomly pick a function based on its request frequency
                function_index = np.random.choice(self._num_functions, p=probabilities)

                cpu_cores = self._cpu_cores[current_minute_index][function_index]
                memory = self._memory[current_minute_index][function_index]
                delay = self._delay[current_minute_index][function_index]

                # Advance start time for the next task
                wait_time_ms = get_wait_time(self._target_qps, self._distribution_type,
                                             burstiness=self._burstiness) * 1000
                start_time_ms += wait_time_ms

                generated_tasks.append({
                    "taskId": task_id,
                    "cores": float(cpu_cores),
                    "memory": int(memory),
                    "disk": 0,
                    "duration": int(delay * 1000),  # Assuming delay is in seconds
                    "startTime": int(start_time_ms),
                    "taskType": "simulated",
                    "mode": "small"  # Serverless data does not have mode to control the task load, so we set to small.
                })
                task_id += 1
                cpu_cores_list.append(float(cpu_cores))
                memory_list.append(int(memory))

                # Stop if we have generated the required number of records
                if len(generated_tasks) >= num_records:
                    break
            if len(generated_tasks) >= num_records:
                break

        print(f"Generated {len(generated_tasks)} serverless tasks.")
        if cpu_cores_list:
            avg_cores = sum(cpu_cores_list) / len(cpu_cores_list)
            avg_mem = sum(memory_list) / len(memory_list)
            print(f"Average cores: {avg_cores:.2f}, Average memory: {avg_mem:.2f}MB")

        return generated_tasks
