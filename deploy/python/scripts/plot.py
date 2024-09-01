import os

import matplotlib.pyplot as plt
import math

from deploy.python.analysis.composited_nodes_metrics import CompositedNodesMetrics
from deploy.python.analysis.scheduler_metrics import SchedulerMetrics

use_caelum = False
experiment_name = "azure"
if use_caelum:
    experiment_name = "caelum_" + experiment_name
else:
    experiment_name = "cloud_lab_" + experiment_name

target_dir = "deploy/resources/figure/{}".format(experiment_name)
if not os.path.exists(target_dir):
    os.makedirs(target_dir)

time_steps = 10
max_checkpoints = 1000
composited_node_host_dir = "deploy/resources/log/node/{}".format(experiment_name)

var_resource_mean_lists = {}
average_resource_mean_lists = {}
max_num_waiting_task_lists = {}
average_num_waiting_task_lists = {}
variance_waiting_task_lists = {}

uncached_scheduler_type = ["sparrow", "random", "cachedSparrow"]
# uncached_scheduler_type = []
cached_scheduler_type = ["dodoor"]
test_cpu_weight = [10.0]
# test_duration_weight = [0.5, 0.25, 0.1]
test_duration_weight = [0.5]

for scheduler_name in os.listdir(composited_node_host_dir):
    node_file = os.path.join(composited_node_host_dir, scheduler_name)
    nodes_metrics = CompositedNodesMetrics(node_file)
    average_resource_mean, var_resource_mean = nodes_metrics.get_average_resource_mean_and_variance()
    max_num_waiting_tasks = nodes_metrics.get_max_waiting_tasks()
    average_num_waiting_tasks = nodes_metrics.get_average_waiting_tasks()
    variance_waiting_tasks = nodes_metrics.get_variance_waiting_tasks()

    scheduler_type = scheduler_name.split("_")[0]
    cpu = scheduler_name.split("_")[6]
    if not (scheduler_type in uncached_scheduler_type or (scheduler_type in cached_scheduler_type and
                                                          float(cpu) in test_cpu_weight)):
        continue

    new_scheduler_name = "{}_{}".format(scheduler_type, cpu)

    if scheduler_type == "dodoor":
        duration_weight = scheduler_name.split("_")[-1]
        if float(duration_weight) not in test_duration_weight:
            continue
        new_scheduler_name = "{}_{}_{}".format(scheduler_type, cpu, duration_weight)

    average_num_waiting_task_lists[scheduler_name] = average_num_waiting_tasks
    max_num_waiting_task_lists[scheduler_name] = max_num_waiting_tasks
    var_resource_mean_lists[scheduler_name] = var_resource_mean
    average_resource_mean_lists[scheduler_name] = average_resource_mean
    variance_waiting_task_lists[scheduler_name] = variance_waiting_tasks

    scheduler_type = new_scheduler_name

    plt.figure(1)
    plt.plot(var_resource_mean[:max_checkpoints], label=scheduler_type)

    plt.figure(2)
    plt.plot(average_resource_mean[:max_checkpoints], label=scheduler_type)

    plt.figure(3)
    plt.plot([max_num_waiting_tasks[i] - average_num_waiting_tasks[i] for i in range(max_checkpoints)],
             label=scheduler_type)

    plt.figure(4)
    plt.plot(variance_waiting_tasks[:max_checkpoints], label=scheduler_type)

    plt.figure(5)
    average_waiting_time = nodes_metrics.get_average_task_waited_duration()
    plt.plot(average_waiting_time[:max_checkpoints], label=scheduler_type)

    plt.figure(10)
    average_cpu_usage, cpu_variance = nodes_metrics.get_cpu_usage_mean_and_variance()
    plt.plot(average_cpu_usage[:max_checkpoints], label=scheduler_type)

    print("scheduler: {}, max resource usage variance: {},"
          " max resource usage means: {}".format(scheduler_type, max(var_resource_mean), max(average_resource_mean)))

plt.figure(1)
plt.legend(loc='best', handlelength=1, frameon=False)
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("average resources usage variance")
plt.savefig("{}/average_usage_variance.png".format(target_dir))

plt.figure(2)
plt.legend(loc='best', handlelength=1, frameon=False)
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("average resources usage mean")
plt.savefig("{}/average_resource_usage_mean.png".format(target_dir))

plt.figure(3)
plt.legend(loc='best', handlelength=1, frameon=False)
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("waiting tasks difference")
plt.savefig("{}/waiting_tasks_max_average_difference.png".format(target_dir))

plt.figure(4)
plt.legend(loc='best', handlelength=1, frameon=False)
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("waiting tasks variance")
plt.savefig("{}/waiting_tasks_variance.png".format(target_dir))

plt.figure(5)
plt.legend(loc='best', handlelength=1, frameon=False)
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("average waiting time")
plt.savefig("{}/average_waiting_time.png".format(target_dir))

plt.figure(10)
plt.legend(loc='best', handlelength=1, frameon=False)
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("average cpu usage")
plt.savefig("{}/average_cpu_usage.png".format(target_dir))

scheduler_host_dir = "deploy/resources/log/scheduler/{}".format(experiment_name)
for scheduler_name in os.listdir(scheduler_host_dir):
    for scheduler_log_file in os.listdir(os.path.join(scheduler_host_dir, scheduler_name)):
        if "scheduler_metrics" in scheduler_log_file:
            scheduler_metrics = SchedulerMetrics(os.path.join(scheduler_host_dir, scheduler_name, scheduler_log_file))
            scheduler_type = scheduler_name.split("_")[0]
            cpu = scheduler_name.split("_")[6]
            new_scheduler_name = "{}_{}".format(scheduler_type, cpu)
            if not (scheduler_type in uncached_scheduler_type or (scheduler_type in cached_scheduler_type and
                                                                  float(cpu) in test_cpu_weight)):
                continue
            if scheduler_type == "dodoor":
                duration_weight = scheduler_name.split("_")[-1]
                print(duration_weight)
                if float(duration_weight) not in test_duration_weight:
                    continue
                new_scheduler_name = "{}_{}_{}".format(new_scheduler_name, cpu, duration_weight)

            num_messages = scheduler_metrics.get_num_messages()
            task_rate_m1 = scheduler_metrics.get_task_rate_m1()

            e2e_latency_avg = scheduler_metrics.get_e2e_latency_avg()
            task_e2e_makespan_duration_avg = scheduler_metrics.get_task_makespan_duration_avg()
            num_finished_tasks = scheduler_metrics.get_finished_tasks()

            num_unfinished_tasks = [submitted - finished for submitted, finished in
                                    zip(scheduler_metrics.get_submitted_tasks(), num_finished_tasks)]

            print("scheduler: {}, final e2e latency avg: {},"
                  " final task e2e makespan duration avg: {}, "
                  " final number messages :{}, ".format(scheduler_type, e2e_latency_avg[max_checkpoints - 1],
                                                        task_e2e_makespan_duration_avg[max_checkpoints - 1]/1000,
                                                        num_messages[max_checkpoints - 1]))

            scheduler_type = new_scheduler_name

            plt.figure(6)
            plt.plot(num_messages[:max_checkpoints], label=scheduler_type)

            plt.figure(7)
            plt.plot(task_rate_m1[:max_checkpoints], label=scheduler_type)

            plt.figure(8)
            plt.plot(e2e_latency_avg[:max_checkpoints], label=scheduler_type)

            plt.figure(9)
            plt.plot([duration / 1000 for duration in task_e2e_makespan_duration_avg[:max_checkpoints]],
                     label=scheduler_type)

            plt.figure(11)
            plt.plot(num_unfinished_tasks[:max_checkpoints], label=scheduler_type)

plt.figure(6)
plt.legend(loc='best', handlelength=1, frameon=False)
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("num scheduling messages")
plt.savefig("{}/num_messages.png".format(target_dir))

plt.figure(7)
plt.legend(loc='best', handlelength=1, frameon=False)
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("task rate m1")
plt.savefig("{}/task_rate_m1.png".format(target_dir))

plt.figure(8)
plt.legend(loc='best', handlelength=1, frameon=False)
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("scheduling latency (in ms) avg")
plt.savefig("{}/e2e_latency_avg.png".format(target_dir))

plt.figure(9)
plt.legend(loc='best', handlelength=1, frameon=False)
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("task e2e makespan duration (in seconds) avg")
plt.savefig("{}/task_e2e_makespan_duration_avg.png".format(target_dir))

plt.figure(11)
plt.legend(loc='best', handlelength=1, frameon=False)
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("num unfinished tasks")
plt.savefig("{}/num_unfinished_tasks.png".format(target_dir))
