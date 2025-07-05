import random
from abc import ABC
import json
import numpy as np

from deploy.python.data.generator.data_generator import DataGenerator
from deploy.python.data.generator.utils import get_wait_time


class TableKeys:
    TASK_FIELD = "tasks"
    TASK_TYPE_ID = "taskTypeId"
    INSTANCE_INFO_ID = "instanceInfo"
    RESOURCE_VECTOR = "resourceVector"
    CORES = "cores"
    MEMORY = "memory"
    DISKS = "disks"
    DURATION = "estimatedDuration"


class FunctionBenchGenerator(DataGenerator, ABC):
    def __init__(self, config_address, target_cluster_qps,
                 task_distribution=None,
                 distribution_type="gamma",
                 burstiness=1.0):
        super().__init__()
        self._config_address = config_address
        config = json.load(open(config_address, 'r'))
        self._task_list = config.get(TableKeys.TASK_FIELD, [])
        assert self._task_list, "Task list is empty in the configuration file."
        self._target_qps = target_cluster_qps
        self._task_distribution = task_distribution if task_distribution else {}
        self._distribution_bucket = []

        self._distribution_type = distribution_type
        self._burstiness = burstiness
        if task_distribution:
            assert sum(task_distribution) == 1, \
                "Task distribution must sum to 1. Provided distribution: {}".format(task_distribution)
            assert len(task_distribution) == len(self._task_list), \
                "Task distribution length must match the number of tasks in the configuration file."

            start_bucket = 0
            for i in range(len(self._task_list)):
                task_type_id = self._task_list[i][TableKeys.TASK_TYPE_ID]
                start_bucket += self._task_distribution.get(task_type_id)
                self._distribution_bucket.append(start_bucket)

    def generate(self, num_records, start_id, max_duration=-1, time_range_in_days=None):
        if time_range_in_days is None:
            time_range_in_days = [0, 1]
        start_time = time_range_in_days[0] * 24 * 3600 * 1000  # Convert to milliseconds
        task_id = start_id
        generated_tasks = []
        cpu_cores_list = []
        memory_list = []

        while len(generated_tasks) < num_records:
            if self._task_distribution:
                random_selected_task_index = random.randint(0, self._distribution_bucket[-1] - 1)
                task_type_index = next(i for i, bucket in enumerate(self._distribution_bucket)
                                    if bucket > random_selected_task_index) - 1
            else:
                task_type_index = random.randint(0, len(self._task_list) - 1)

            task_waiting_time = get_wait_time(self._target_qps,
                                              self._distribution_type,
                                              self._burstiness) * 1000
            start_time += int(task_waiting_time)
            task_type = self._task_list[task_type_index]
            instance_info = task_type.get(TableKeys.INSTANCE_INFO_ID, {})
            first_instance = next(iter(instance_info.values()))
            resource_vector = first_instance.get(TableKeys.RESOURCE_VECTOR, {})
            cores = resource_vector.get(TableKeys.CORES)
            memory = resource_vector.get(TableKeys.MEMORY)
            disk = resource_vector.get(TableKeys.DISKS, 0)
            duration = first_instance.get(TableKeys.DURATION)

            generated_tasks.append({
                "taskId": task_id,
                "cores": float(cores),
                "memory": int(memory),
                "disk": int(disk),
                "duration": int(duration),
                "startTime": start_time,
                "taskType": task_type[TableKeys.TASK_TYPE_ID],
            })
            task_id += 1
            cpu_cores_list.append(float(cores))
            memory_list.append(int(memory))
        print("Average cores: {}, Average memory: {}".format(sum(cpu_cores_list) / len(cpu_cores_list),
                                                             sum(memory_list) / len(memory_list)))
        return generated_tasks
