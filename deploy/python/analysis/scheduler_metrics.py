import re


class SchedulerMetrics:
    message_counter_pattern = re.compile("type=COUNTER, name=scheduler.metrics.num.messages, count=\d+")
    e2e_latency_pattern = re.compile("type=HISTOGRAM, name=scheduler.metrics.tasks.e2e.scheduling.latency.histograms, "
                                     "count=\d+, min=\d+, "
                                     "max=\d+, "
                                     "mean=\d+.\d+, "
                                     "stddev=\d+.\d+, "
                                     "p50=\d+.\d+, "
                                     "p75=\d+.\d+, "
                                     "p95=\d+.\d+, "
                                     "p98=\d+.\d+, "
                                     "p99=\d+.\d+, "
                                     "p999=\d+.\d+")
    task_rate_pattern = re.compile("type=METER, name=scheduler.metrics.tasks.rate, count=\d+, "
                                   "m1_rate=\d+.\d+, m5_rate=\d+.\d+, m15_rate=\d+.\d+, mean_rate=\d+.\d+, "
                                   "rate_unit=events/second")

    def __init__(self, log_file):
        self.metrics = {"num_messages": [],
                        "e2e_latency_avg": [],
                        "e2e_latency_max": [],
                        "e2e_latency_min": [],
                        "e2e_latency_std": [],
                        "e2e_latency_p50": [],
                        "e2e_latency_p99": [],
                        "task_rate_mean": []}
        self.log_file = log_file

    def parse(self):
        with open(self.log_file, 'r') as f:
            for line in f.readlines():
                if self.message_counter_pattern.match(line):
                    self.metrics["num_messages"].append(int(line.split(",")[2].split("=")[1]))
                elif self.task_rate_pattern.match(line):
                    self.metrics["task_rate_mean"].append(float(line.split(",")[6].split("=")[1]))
                elif self.e2e_latency_pattern.match(line):
                    self.metrics["e2e_latency_avg"].append(float(line.split(",")[5].split("=")[1]))
                    self.metrics["e2e_latency_max"].append(float(line.split(",")[4].split("=")[1]))
                    self.metrics["e2e_latency_min"].append(float(line.split(",")[3].split("=")[1]))
                    self.metrics["e2e_latency_std"].append(float(line.split(",")[6].split("=")[1]))
                    self.metrics["e2e_latency_p50"].append(float(line.split(",")[7].split("=")[1]))
                    self.metrics["e2e_latency_p99"].append(float(line.split(",")[11].split("=")[1]))
        return self.metrics

    def get_num_messages(self):
        return self.metrics["num_messages"]

    def get_task_rate_mean(self):
        return self.metrics["task_rate_mean"]

    def get_e2e_latency_avg(self):
        return self.metrics["e2e_latency_avg"]

    def get_e2e_latency_max(self):
        return self.metrics["e2e_latency_max"]

    def get_e2e_latency_p99(self):
        return self.metrics["e2e_latency_p99"]

    def get_e2e_latency_p50(self):
        return self.metrics["e2e_latency_p50"]
