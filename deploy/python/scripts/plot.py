import os

import matplotlib.pyplot as plt

from deploy.python.analysis.composited_nodes_metrics import CompositedNodesMetrics
from deploy.python.analysis.scheduler_metrics import SchedulerMetrics

use_caelum = False
num_nodes = 30
experiment_name = "exp_azure_full_replay_{}/dodoor".format(num_nodes)
if use_caelum:
    experiment_name += "_caelum"
else:
    experiment_name += "_cloud_lab"
target_dir = "deploy/resources/figure/{}".format(experiment_name)
if not os.path.exists(target_dir):
    os.makedirs(target_dir)

time_steps = 10
max_checkpoints = 70
composited_node_host_file = "deploy/resources/log/node/{}".format(experiment_name)

nodes_metrics = CompositedNodesMetrics(composited_node_host_file)
average_resource_mean, var_resource_mean = nodes_metrics.get_average_resource_mean_and_variance()

# plt.plot(average_cpu, label="average cpu usage")
plt.figure(1)
plt.plot(var_resource_mean[:max_checkpoints], label="average resource variance")
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("average resources usage variance")
plt.savefig("{}/average_usage_variance.png".format(target_dir))

plt.figure(2)
plt.plot(average_resource_mean[:max_checkpoints], label="cpu usage")
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("average resources usage mean")
plt.savefig("{}/average_resource_usage_mean.png".format(target_dir))

max_num_waiting_tasks = nodes_metrics.get_max_waiting_tasks()
average_num_waiting_tasks = nodes_metrics.get_average_waiting_tasks()

plt.figure(3)
plt.plot([max_num_waiting_tasks[i] - average_num_waiting_tasks[i] for i in range(max_checkpoints)],
         label="(max - average) waiting tasks")
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("waiting tasks difference")
plt.savefig("{}/waiting_tasks_max_average_difference.png".format(target_dir))

plt.figure(4)
variance_waiting_tasks = nodes_metrics.get_variance_waiting_tasks()
plt.plot(variance_waiting_tasks[:max_checkpoints], label="waiting tasks variance")
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("waiting tasks variance")
plt.savefig("{}/waiting_tasks_variance.png".format(target_dir))

scheduler_host_file = "deploy/resources/log/scheduler/{}".format(experiment_name)
scheduler_metrics = None
for scheduler_log in os.listdir(scheduler_host_file):
    scheduler_metrics = SchedulerMetrics(os.path.join(scheduler_host_file, scheduler_log))
scheduler_metrics.parse()

num_messages = scheduler_metrics.get_num_messages()
task_rate_m1 = scheduler_metrics.get_task_rate_m1()
e2e_latency_avg = scheduler_metrics.get_e2e_latency_avg()

plt.figure(5)
plt.plot(num_messages[:max_checkpoints], label="num messages")
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("scheduler num messages")
plt.savefig("{}/scheduler_num_messages.png".format(target_dir))

plt.figure(6)
plt.plot(task_rate_m1[:max_checkpoints], label="task rate m1")
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("scheduler task rate in last one minute")
plt.savefig("{}/scheduler_task_rate_m1.png".format(target_dir))

plt.figure(7)
e2e_latency_avg_in_seconds = [e2e_latency / 1000 for e2e_latency in e2e_latency_avg]
plt.plot(e2e_latency_avg[:max_checkpoints], label="e2e latency avg")
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("scheduler e2e latency avg")
plt.savefig("{}/scheduler_e2e_latency_avg.png".format(target_dir))