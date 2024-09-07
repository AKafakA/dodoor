import re


class SchedulerMetrics:
    message_counter_pattern = re.compile("type=COUNTER, name=scheduler.metrics.num.messages, count=\d+")
    e2e_latency_pattern = re.compile("type=HISTOGRAM, name=scheduler.metrics.tasks.e2e.scheduling.latency.histograms, count=\d+, min=\-?\d+, "
                                     "max=\d+, "
                                     "mean=\d+.\d+(E-\d+)?, "
                                     "stddev=\d+.\d+(E-\d+)?, "
                                     "p50=\d+.\d+(E-\d+)?, "
                                     "p75=\d+.\d+(E-\d+)?, "
                                     "p95=\d+.\d+(E-\d+)?, "
                                     "p98=\d+.\d+(E-\d+)?, "
                                     "p99=\d+.\d+(E-\d+)?, "
                                     "p999=\d+.\d+(E-\d+)?")

    task_rate_pattern = re.compile("type=METER, name=scheduler.metrics.tasks.rate, count=\d+, "
                                   "m1_rate=\d+.\d+(E-\d+)?, m5_rate=\d+.\d+(E-\d+)?, "
                                   "m15_rate=\d+.\d+(E-\d+)?, mean_rate=\d+.\d+(E-\d+)?, "
                                   "rate_unit=events/second")

    e2e_makespan_pattern = re.compile("type=HISTOGRAM, name=scheduler.metrics.tasks.e2e.makespan.latency.histograms, "
                                     "count=\d+, min=\d+, "
                                     "max=\d+, "
                                     "mean=\d+.\d+(E-\d+)?, "
                                     "stddev=\d+.\d+(E-\d+)?, "
                                     "p50=\d+.\d+(E-\d+)?, "
                                     "p75=\d+.\d+(E-\d+)?, "
                                     "p95=\d+.\d+(E-\d+)?, "
                                     "p98=\d+.\d+(E-\d+)?, "
                                     "p99=\d+.\d+(E-\d+)?, "
                                     "p999=\d+.\d+(E-\d+)?")
    finished_tasks_pattern = re.compile("type=COUNTER, name=scheduler.metrics.tasks.finished.count, count=\d+")

    def __init__(self, log_file):
        self.metrics = {"num_messages": [],
                        "e2e_latency_avg": [],
                        "e2e_latency_max": [],
                        "e2e_latency_min": [],
                        "e2e_latency_std": [],
                        "e2e_latency_p50": [],
                        "e2e_latency_p99": [],
                        "e2e_latency_count": [],
                        "task_rate_mean": [],
                        "task_rate_m1": [],
                        "task_makespan_duration_avg": [],
                        "submitted_tasks": [],
                        "finished_tasks": []}
        self.log_file = log_file
        self.parse()

    def parse(self):
        with open(self.log_file, 'r') as f:
            for line in f.readlines():
                if self.message_counter_pattern.match(line):
                    self.metrics["num_messages"].append(int(line.split(",")[2].split("=")[1]))
                elif line.startswith("type=METER, name=scheduler.metrics.tasks.rate"):
                    self.metrics["task_rate_mean"].append(float(line.split(",")[6].split("=")[1]))
                    self.metrics["task_rate_m1"].append(float(line.split(",")[3].split("=")[1]))
                    self.metrics["submitted_tasks"].append(int(line.split(",")[2].split("=")[1]))
                elif self.e2e_latency_pattern.match(line):
                    self.metrics["e2e_latency_avg"].append(float(line.split(",")[5].split("=")[1]))
                    self.metrics["e2e_latency_max"].append(float(line.split(",")[4].split("=")[1]))
                    self.metrics["e2e_latency_min"].append(float(line.split(",")[3].split("=")[1]))
                    self.metrics["e2e_latency_std"].append(float(line.split(",")[6].split("=")[1]))
                    self.metrics["e2e_latency_p50"].append(float(line.split(",")[7].split("=")[1]))
                    self.metrics["e2e_latency_p99"].append(float(line.split(",")[11].split("=")[1]))
                    self.metrics["e2e_latency_count"].append(int(line.split(",")[2].split("=")[1]))
                elif self.e2e_makespan_pattern.match(line):
                    self.metrics["task_makespan_duration_avg"].append(float(line.split(",")[5].split("=")[1]))
                elif self.finished_tasks_pattern.match(line):
                    self.metrics["finished_tasks"].append(int(line.split(",")[2].split("=")[1]))
        return self.metrics

    def get_num_messages(self):
        return self.metrics["num_messages"]

    def get_task_rate_mean(self):
        return self.metrics["task_rate_mean"]

    def get_task_rate_m1(self):
        return self.metrics["task_rate_m1"]

    def get_e2e_latency_avg(self):
        return self.metrics["e2e_latency_avg"]

    def get_e2e_latency_max(self):
        return self.metrics["e2e_latency_max"]

    def get_e2e_latency_p99(self):
        return self.metrics["e2e_latency_p99"]

    def get_e2e_latency_p50(self):
        return self.metrics["e2e_latency_p50"]

    def get_e2e_latency_count(self):
        return self.metrics["e2e_latency_count"]

    def get_task_makespan_duration_avg(self):
        return self.metrics["task_makespan_duration_avg"]

    def get_finished_tasks(self):
        return self.metrics["finished_tasks"]

    def get_submitted_tasks(self):
        return self.metrics["submitted_tasks"]

