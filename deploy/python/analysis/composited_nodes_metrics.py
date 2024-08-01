import os
from typing import List

import numpy as np

from deploy.python.analysis.node_metrics import NodeMetrics


def get_nodes_metrics_lists(metrics_name, metrics_length, nodes: List[NodeMetrics]):
    nodes_metrics = []
    for i in range(metrics_length):
        nodes_metric = []
        for node in nodes:
            nodes_metric.append(node.metrics[metrics_name][i])
        nodes_metrics.append(nodes_metric)
    return nodes_metrics


def get_average_nodes_metrics(metrics_name, metrics_length, nodes: List[NodeMetrics]):
    nodes_metrics_lists = get_nodes_metrics_lists(metrics_name, metrics_length, nodes)
    average_nodes_metrics = [sum(nodes_metrics) / len(nodes) for nodes_metrics in nodes_metrics_lists]
    return average_nodes_metrics


def get_max_nodes_metrics(metrics_name, metrics_length, nodes: List[NodeMetrics]):
    nodes_metrics_lists = get_nodes_metrics_lists(metrics_name, metrics_length, nodes)
    max_nodes_metrics = [max(nodes_metrics) for nodes_metrics in nodes_metrics_lists]
    return max_nodes_metrics


def get_total_nodes_metrics(metrics_name, metrics_length, nodes: List[NodeMetrics]):
    nodes_metrics_lists = get_nodes_metrics_lists(metrics_name, metrics_length, nodes)
    total_nodes_metrics = [sum(nodes_metrics) for nodes_metrics in nodes_metrics_lists]
    return total_nodes_metrics


def get_variance_nodes_metrics(metrics_name, metrics_length, nodes: List[NodeMetrics]):
    nodes_metrics_lists = get_nodes_metrics_lists(metrics_name, metrics_length, nodes)
    variance_nodes_metrics = [np.var(np.array(nodes_metrics)) for nodes_metrics in nodes_metrics_lists]
    return variance_nodes_metrics


class CompositedNodesMetrics:
    def __init__(self, node_log_dir, id="default"):
        self.id = id
        self.node_metrics = []
        self.min_start_step = 100000
        self.max_length = 0
        for filename in os.listdir(os.path.join(os.getcwd(), node_log_dir)):
            if filename.endswith(".log"):
                node_id = int(filename.split("_")[-1].split(".")[0])
                log_file_path = os.path.join(os.getcwd(), node_log_dir, filename)
                node_metric = NodeMetrics(log_file_path, node_id)
                node_metric.parse()
                self.node_metrics.append(node_metric)
                self.min_start_step = min(node_metric.start_step, self.min_start_step)
                self.max_length = max(node_metric.length, self.max_length)

        for node_metric in self.node_metrics:
            node_metric.calibrate(self.min_start_step, self.max_length)
        self.length = self.max_length - self.min_start_step

    def get_num_nodes(self):
        return len(self.node_metrics)

    def get_total_messages(self):
        return get_total_nodes_metrics("message", self.length, self.node_metrics)

    def get_total_waiting_tasks(self):
        return get_total_nodes_metrics("num_waiting_tasks", self.length, self.node_metrics)

    def get_max_waiting_tasks(self):
        return get_max_nodes_metrics("num_waiting_tasks", self.length, self.node_metrics)

    def get_average_waiting_tasks(self):
        return get_average_nodes_metrics("num_waiting_tasks", self.length, self.node_metrics)

    def get_average_task_rate_average(self):
        return get_average_nodes_metrics("task_rate_mean", self.length, self.node_metrics)

    def get_average_task_waited_duration(self):
        total_waited_duration = get_total_nodes_metrics("duration_task_waited_total", self.length, self.node_metrics)
        total_waited_tasks = get_total_nodes_metrics("count_tasks_waited", self.length, self.node_metrics)
        return [total_waited_duration[i] / total_waited_tasks[i]
                if total_waited_tasks[i] != 0 else 0 for i in range(self.length)]

    def get_total_finished_tasks(self):
        return get_total_nodes_metrics("num_finished_tasks", self.length, self.node_metrics)

    def get_cpu_usage_mean_and_variance(self):
        return get_average_nodes_metrics("cpu_usage", self.length, self.node_metrics), \
            get_variance_nodes_metrics("cpu_usage", self.length, self.node_metrics)

    def get_mem_usage_mean_and_variance(self):
        return get_average_nodes_metrics("mem_usage", self.length, self.node_metrics), \
            get_variance_nodes_metrics("mem_usage", self.length, self.node_metrics)

    def get_disk_usage_mean_and_variance(self):
        return get_average_nodes_metrics("disk_usage", self.length, self.node_metrics), \
            get_variance_nodes_metrics("disk_usage", self.length, self.node_metrics)

    def get_average_resource_mean_and_variance(self, include_disk=False):
        cpu_mean, cpu_variance = self.get_cpu_usage_mean_and_variance()
        mem_mean, mem_variance = self.get_mem_usage_mean_and_variance()
        if include_disk:
            disk_mean, disk_variance = self.get_disk_usage_mean_and_variance()
            average_mean = [sum(x) / 3 for x in zip(cpu_mean, mem_mean, disk_mean)]
            average_variance = [sum(x) / 3 for x in zip(cpu_variance, mem_variance, disk_variance)]
        else:
            average_mean = [sum(x) / 2 for x in zip(cpu_mean, mem_mean)]
            average_variance = [sum(x) / 2 for x in zip(cpu_variance, mem_variance)]
        return average_mean, average_variance

    def get_variance_waiting_tasks(self):
        return get_variance_nodes_metrics("num_waiting_tasks", self.length, self.node_metrics)
