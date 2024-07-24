import math
from abc import ABC

from deploy.python.data.generator.azure.azure_sqlite_processor import AzureSqliteProcessor, TableKeys
from deploy.python.data.generator.data_generator import DataGenerator


class AzureDataGenerator(DataGenerator, ABC):
    def __init__(self, db_path, output_path, machine_ids=None,
                 max_cores=24,
                 max_memory=30720,
                 max_disk=307200, time_interval=86400000):
        super().__init__(output_path)
        self.machine_ids = [0]
        if machine_ids is not None:
            self.machine_ids = machine_ids
        self.sqlite_processor = AzureSqliteProcessor(db_path)
        self.max_cores = max_cores
        self.max_memory = max_memory
        self.max_disk = max_disk
        self.time_interval = time_interval

    def generate(self, num_records, start_id, max_duration=-1, time_range_in_days=None):
        data = []
        if time_range_in_days is None:
            time_range_in_days = [0, 0.1]
        for machine_id in self.machine_ids:
            vm_requests = self.sqlite_processor.get_vm_resource_requests_in_batch(machine_id=machine_id,
                                                                                  num_requests_per_machine=num_records)
            for vm in vm_requests:
                task_id = vm[TableKeys.VM_ID]
                if None in vm.values() or task_id < start_id:
                    continue
                start_time = vm[TableKeys.START_TIME]
                if start_time < time_range_in_days[0] or start_time > time_range_in_days[1]:
                    continue
                start_time *= self.time_interval
                duration = (vm[TableKeys.END_TIME] - vm[TableKeys.START_TIME]) * self.time_interval
                if 0 < max_duration < duration:
                    continue
                cores = vm[TableKeys.RESOURCE_TYPE[0]] * self.max_cores
                memory = vm[TableKeys.RESOURCE_TYPE[1]] * self.max_memory
                disk = vm[TableKeys.RESOURCE_TYPE[2]] * self.max_disk
                data.append({
                    "taskId": task_id,
                    "cores": math.ceil(cores),
                    "memory": math.ceil(memory),
                    "disk": math.ceil(disk),
                    "duration": int(duration),
                    "startTime": int(start_time)
                })
        data = sorted(data, key=lambda x: x["taskId"])[:num_records]
        return data

