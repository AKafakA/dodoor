import math
from abc import ABC

from deploy.python.data.generator.azure.azure_sqlite_processor import AzureSqliteProcessor, TableKeys
from deploy.python.data.generator.data_generator import DataGenerator
from deploy.python.data.generator.utils import get_wait_time

MIN_CORES = 1


class AzureDataGenerator(DataGenerator, ABC):
    def __init__(self, db_path, machine_ids=None,
                 max_cores=28,
                 max_memory=30720,
                 max_disk=307200,
                 time_interval=24 * 60 * 60 * 1000,
                 target_qps=-1,
                 distribution_type="gamma",
                 burstiness=1.0):
        super().__init__()
        self.machine_ids = [0]
        if machine_ids is not None:
            self.machine_ids = machine_ids
        self.sqlite_processor = AzureSqliteProcessor(db_path)
        self.max_cores = max_cores
        self.max_memory = max_memory
        self.max_disk = max_disk
        self.time_interval = time_interval
        self.target_qps = target_qps
        self.distribution_type = distribution_type
        self.burstiness = burstiness

    def generate(self, num_records, start_id, max_duration=-1, time_range_in_days=None,
                 max_cores=8, max_memory=62 * 1024, max_disk=0, reassign_ids=True):
        """
            timeline_compress_ratio is used to compress the timeline to smaller value for fasting replay.
            e.g if last events is submitted in 14th days, so the timeline should be 60000 * 60 * 24 * 14
            but if want to replay it within half day, then the time_compress_ratio should be 1 / (14 * 2)
            Similarly, time_shift is used to shift the time by the mod functions. -1 means no shifting
        """
        cpu_cores_list = []
        memory_list = []
        task_ids = {}
        data = []
        duration_list = []
        exp_timeline = 0
        if time_range_in_days is None:
            time_range_in_days = [0, 0.1]
        for machine_id in self.machine_ids:
            if len(data) >= num_records:
                break
            vm_requests = self.sqlite_processor.get_vm_resource_requests_in_batch(machine_id=machine_id,
                                                                                  num_requests_per_machine=-1)
            for vm in vm_requests:
                task_id = vm[TableKeys.VM_ID]
                if None in vm.values() or task_id < start_id or task_ids.get(task_id, False):
                    continue
                start_time = vm[TableKeys.START_TIME]
                end_time = vm[TableKeys.END_TIME]
                if not (time_range_in_days[0] <= start_time <= end_time <= time_range_in_days[1]):
                    continue
                # convert duration from day to milliseconds
                duration = (vm[TableKeys.END_TIME] - vm[TableKeys.START_TIME]) * self.time_interval
                if 0 < max_duration < duration:
                    continue
                cores = vm[TableKeys.RESOURCE_TYPE[0]] * self.max_cores
                memory = vm[TableKeys.RESOURCE_TYPE[1]] * self.max_memory
                disk = vm[TableKeys.RESOURCE_TYPE[2]] * self.max_disk

                if cores <= 0 and memory <= 0 and disk <= 0:
                    continue
                # generate the override start time specify target qps
                if self.target_qps > 0:
                    # if given target qps, we need to override the start time with timeline
                    # else use converted start time into milliseconds
                    wait_time = int(get_wait_time(self.target_qps, self.distribution_type, self.burstiness) * 1000)
                    exp_timeline += wait_time
                    start_time = exp_timeline
                else:
                    start_time -= time_range_in_days[0]
                    start_time *= self.time_interval

                data.append({
                    "taskId": task_id,
                    "cores": float(cores),
                    "memory": int(memory),
                    "disk": int(disk),
                    "duration": int(duration),
                    "startTime": int(start_time),
                    "taskType": "simulated",
                    "mode": "medium"  # Azure data does not have mode to control the task load, so we set to medium.
                })
                cpu_cores_list.append(int(cores))
                memory_list.append(int(memory))
                duration_list.append(int(duration))  # Convert duration to seconds
                task_ids[task_id] = True
                if len(data) >= num_records:
                    break
        data = sorted(data, key=lambda x: x["startTime"])
        if reassign_ids:
            for i in range(len(data)):
                data[i]["taskId"] = i
        print("Average cores: {}, Average memory: {}, Average Duration:{} ms ".format(sum(cpu_cores_list) / len(cpu_cores_list),
                                                             sum(memory_list) / len(memory_list),
                                                             sum(duration_list) / len(duration_list)))
        return data
