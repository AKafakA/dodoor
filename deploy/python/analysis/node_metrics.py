import re


def calibrate_metrics(start_point, end_point, metrics):
    new_metrics = metrics[start_point:]
    new_metrics.extend([metrics[-1]] * (end_point - len(metrics)))
    return new_metrics


class NodeMetrics:
    message_counter_pattern = re.compile("type=COUNTER, name=node.metrics.num.messages, count=\d+")
    num_waiting_tasks_pattern = re.compile("type=COUNTER, name=node.metrics.tasks.waiting.count, count=\d+")
    num_finished_tasks_pattern = re.compile("type=COUNTER, name=node.metrics.tasks.finished.count, count=\d+")
    resource_usage_pattern = re.compile(
        "Time\(in Seconds\) OSM: \d+ CPU usage: \d+.\d+(E-\d)? Memory usage: \d+.\d+(E-\d)? Disk usage: \d+.\d+(E-\d)?")
    task_rate_pattern = re.compile("type=METER, name=node.metrics.tasks.rate, count=\d+, "
                                   "m1_rate=\d+.\d+, m5_rate=\d+.\d+, m15_rate=\d+.\d+, mean_rate=\d+.\d+, "
                                   "rate_unit=events/second")

    task_waiting_duration_pattern = re.compile("type=HISTOGRAM, name=node.metrics.tasks.wait.time.histograms, "
                                               "count=\d+, min=\d+, max=\d+, mean=\d+.\d+, "
                                               "stddev=\d+.\d+, "
                                               "p50=\d+.\d+, "
                                               "p75=\d+.\d+, "
                                               "p95=\d+.\d+, "
                                               "p98=\d+.\d+, "
                                               "p99=\d+.\d+, p999=\d+.\d+")

    def __init__(self, log_file, node_id):
        self.log_file = log_file
        self.node_id = node_id
        self.metrics = {"num_waiting_tasks": [],
                        "num_finished_tasks": [],
                        "duration_task_waited_avg": [],
                        "duration_task_waited_total": [],
                        "count_tasks_waited": [],
                        "cpu_usage": [], "mem_usage": [], "disk_usage": [],
                        "message": []}
        self.length = 0
        # track when the node start to consume the tasks
        self.start_step = 0

    def parse(self):
        with open(self.log_file, 'r') as f:
            for line in f.readlines():
                if self.message_counter_pattern.match(line):
                    self.metrics["message"].append(int(line.split(",")[2].split("=")[1]))
                elif self.num_waiting_tasks_pattern.match(line):
                    wait_task = int(line.split(",")[2].split("=")[1])
                    self.metrics["num_waiting_tasks"].append(wait_task)
                    if wait_task > 0 >= self.start_step:
                        self.start_step = len(self.metrics["num_waiting_tasks"])
                elif self.num_finished_tasks_pattern.match(line):
                    self.metrics["num_finished_tasks"].append(int(line.split(",")[2].split("=")[1]))
                elif self.resource_usage_pattern.match(line):
                    cpu = float(line.split(" ")[6])
                    mem = float(line.split(" ")[9])
                    disk = float(line.split(" ")[12])
                    self.metrics["cpu_usage"].append(cpu)
                    self.metrics["mem_usage"].append(mem)
                    self.metrics["disk_usage"].append(disk)
                elif self.task_waiting_duration_pattern.match(line):
                    average_duration = float(line.split(",")[5].split("=")[1])
                    self.metrics["duration_task_waited_avg"].append(average_duration)
                    counted_tasks = int(line.split(",")[2].split("=")[1])
                    self.metrics["duration_task_waited_total"].append(average_duration * counted_tasks)
                    self.metrics["count_tasks_waited"].append(counted_tasks)
        self.length = min([len(metric) for metric in self.metrics.values()])

    def calibrate(self, start_point, length):
        for key, metric in self.metrics.items():
            self.metrics[key] = self.metrics[key][start_point:start_point + length]
