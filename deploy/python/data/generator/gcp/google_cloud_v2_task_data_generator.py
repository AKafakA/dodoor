import math
from abc import ABC
import json
import os

from deploy.python.data.generator.data_generator import DataGenerator

MIN_CORES = 1


class GoogleCloudV2TaskDataGenerator(DataGenerator, ABC):
    def __init__(self,
                 event_json_path,
                 max_cores=48,
                 max_memory=384 * 1024,
                 max_disk=384 * 1024):
        super().__init__()
        self.max_cores = max_cores
        self.max_memory = max_memory
        self.max_disk = max_disk
        self.event_json_path = event_json_path
        self.tasks = self.parse()

    def parse(self):
        tasks = []
        task_id = 0
        job_id_index = {}
        for event_file in os.listdir(self.event_path_dir):
            if not event_file.endswith(".json"):
                continue
            with open(os.path.join(self.event_json_path, event_file), "r") as f:
                for line in f:
                    data = json.loads(line)
                    if "resource_request" not in data:
                        continue
                    time = int(data["time"]) / 1000
                    event_type = data["type"]
                    collection_id = data["collection_id"]
                    instance_index = int(data["instance_index"])
                    cpu = data["resource_request"]["cpus"]
                    memory = data["resource_request"]["memory"]

                    if event_type == "0" and cpu != "" and memory != "":
                        tasks.append({
                            "taskId": task_id,
                            "cores": float(cpu),
                            "memory": float(memory),
                            "disk": 0,
                            "duration": -1,
                            "startTime": time,
                            "scheduledTime": -1,
                        })
                        task_per_job = job_id_index.get(collection_id, {})
                        task_per_job[instance_index] = task_id
                        job_id_index[collection_id] = task_per_job
                        task_id += 1
                    elif event_type == "3" and job_id_index.get(collection_id, False) and job_id_index[collection_id].get(instance_index, False):
                        task_id = job_id_index[collection_id][instance_index]
                        tasks[task_id]["scheduledTime"] = time
                    elif event_type == "6" and job_id_index.get(collection_id, False) and job_id_index[collection_id].get(instance_index, False):
                        task_id = job_id_index[collection_id][instance_index]
                        if tasks[task_id]["scheduledTime"] != -1:
                            tasks[task_id]["duration"] = time - tasks[task_id]["scheduledTime"]
            tasks = sorted(tasks, key=lambda x: x["startTime"])
            tasks = [task for task in tasks if task["duration"] != -1]
        return tasks

    def generate(self, num_records, start_id, max_duration=-1, time_range_in_days=None,
                 timeline_compress_ratio=1.0, time_shift=-1,
                 max_cores=-1,
                 max_memory=-1,
                 max_disk=-1,
                 take_before_request=False,
                 cores_scale=1, memory_scale=1, disk_scale=1,
                 reassigned_ids=True):
        selected_tasks = []
        for task in self.tasks.copy():
            task_id = task["taskId"]
            if time_range_in_days is None:
                time_range_in_days = [0, 24]
            start_time = task["startTime"]
            if start_time > time_range_in_days[1] * 24 * 60 * 60 * 1000:
                continue
            if start_time <= time_range_in_days[0] * 24 * 60 * 60 * 1000:
                if take_before_request:
                    start_time = time_range_in_days[0] * 24 * 60 * 60 * 1000
                else:
                    continue
            if time_shift > 0:
                start_time = start_time + time_shift * 1000 * 60 * 60 * 24
            start_time = start_time * timeline_compress_ratio - time_range_in_days[0] * 24 * 60 * 60 * 1000
            duration = task["duration"]
            if 0 < max_duration < duration or duration < 0:
                continue
            cores = task["cores"] * self.max_cores
            memory = task["memory"] * self.max_memory
            disk = task["disk"] * self.max_disk
            if max_cores > 0:
                cores = min(cores * cores_scale, max_cores)
            if max_memory > 0:
                memory = min(memory * memory_scale, max_memory)
            if max_disk > 0:
                disk = min(disk * disk_scale, max_disk)
            selected_tasks.append({
                "taskId": task_id,
                "cores": math.ceil(cores),
                "memory": math.ceil(memory),
                "disk": math.ceil(disk),
                "duration": int(duration),
                "startTime": int(start_time),
            })
        data = sorted(selected_tasks, key=lambda x: x["startTime"])[:num_records]
        if reassigned_ids:
            for i in range(len(data)):
                data[i]["taskId"] = i + start_id
        return data
