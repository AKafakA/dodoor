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

scheduler_name_map = {"sparrow": "PowerOfTwo", "random": "Random", "prequal": "Prequal",
                      "cachedSparrow": "CachedPowerOfTwo",
                      "dodoor": "Dodoor"}

time_steps = 10
max_checkpoints = 1400
composited_node_host_dir = "deploy/resources/log/node/{}".format(experiment_name)

var_resource_mean_lists = {}
average_resource_mean_lists = {}
max_num_waiting_task_lists = {}
average_num_waiting_task_lists = {}
variance_waiting_task_lists = {}

uncached_scheduler_type = ["sparrow", "random", "prequal", "cachedSparrow"]
# uncached_scheduler_type = []
cached_scheduler_type = ["dodoor"]
test_cpu_weight = [50.0, 10.0, 25.0]
# test_duration_weight = [0.5, 0.25, 0.1]
test_duration_weight = [0.5, 0.75, 0.25]

all_nodes_metrics = {}
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

    scheduler_type = scheduler_name_map[scheduler_type]

    all_nodes_metrics[scheduler_type] = {"average_resource_mean": average_resource_mean,
                                         "var_resource_mean": var_resource_mean,
                                         "max_num_waiting_tasks": max_num_waiting_tasks,
                                         "average_num_waiting_tasks": average_num_waiting_tasks,
                                         "variance_waiting_tasks": variance_waiting_tasks}

    plt.figure(3)
    plt.plot([max_num_waiting_tasks[i] - average_num_waiting_tasks[i] for i in range(max_checkpoints)],
             label=scheduler_type)

    plt.figure(4)
    plt.plot(variance_waiting_tasks[:max_checkpoints], label=scheduler_type)

    plt.figure(5)
    average_waiting_time = nodes_metrics.get_average_task_waited_duration()
    plt.plot(average_waiting_time[:max_checkpoints], label=scheduler_type)

    print("scheduler: {}, max resource usage variance: {},"
          " max resource usage means: {}".format(scheduler_type, max(var_resource_mean), max(average_resource_mean)))

plt.figure(3)
# plt.legend(loc='best', handlelength=1, frameon=False)
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("waiting tasks difference")
plt.savefig("{}/waiting_tasks_max_average_difference.png".format(target_dir))

plt.figure(4)
# plt.legend(loc='best', handlelength=1, frameon=False)
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("waiting tasks variance")
plt.savefig("{}/waiting_tasks_variance.png".format(target_dir))

plt.figure(5)
# plt.legend(loc='best', handlelength=1, frameon=False)
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("average waiting time")
plt.savefig("{}/average_waiting_time.png".format(target_dir))

scheduler_host_dir = "deploy/resources/log/scheduler/{}".format(experiment_name)
schedulers_metrics = {}
for scheduler_name in os.listdir(scheduler_host_dir):
    for scheduler_log_file in os.listdir(os.path.join(scheduler_host_dir, scheduler_name)):
        if "scheduler_metrics" in scheduler_log_file:
            nodes_metrics = SchedulerMetrics(os.path.join(scheduler_host_dir, scheduler_name, scheduler_log_file))
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

            num_messages = nodes_metrics.get_num_messages()
            task_rate_m1 = nodes_metrics.get_task_rate_m1()

            e2e_latency_avg = nodes_metrics.get_e2e_latency_avg()
            task_e2e_makespan_duration_avg = nodes_metrics.get_task_makespan_duration_avg()
            num_finished_tasks = nodes_metrics.get_finished_tasks()

            num_unfinished_tasks = [submitted - finished for submitted, finished in
                                    zip(nodes_metrics.get_submitted_tasks(), num_finished_tasks)]

            print("scheduler: {}, final e2e latency avg: {},"
                  " final task e2e makespan duration avg: {}, "
                  " final number messages :{}, ".format(scheduler_type, e2e_latency_avg[max_checkpoints - 1],
                                                        task_e2e_makespan_duration_avg[max_checkpoints - 1] / 1000,
                                                        num_messages[max_checkpoints - 1]))

            scheduler_type = scheduler_name_map[scheduler_type]

            schedulers_metrics[scheduler_type] = {"num_messages": num_messages, "task_rate_m1": task_rate_m1,
                                                  "e2e_latency_avg": e2e_latency_avg,
                                                  "task_e2e_makespan_duration_avg": task_e2e_makespan_duration_avg,
                                                  "num_unfinished_tasks": num_unfinished_tasks}

            plt.figure(6)
            plt.plot(num_messages[:max_checkpoints], label=scheduler_type)

            plt.figure(7)
            plt.plot(task_rate_m1[:max_checkpoints], label=scheduler_type)


plt.figure(6)
# plt.legend(loc='best', handlelength=1, frameon=False)
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("num scheduling messages")
plt.savefig("{}/num_messages.png".format(target_dir))

plt.figure(7)
# plt.legend(loc='best', handlelength=1, frameon=False)
plt.xlabel("{} seconds".format(time_steps))
plt.ylabel("task rate m1")
plt.savefig("{}/task_rate_m1.png".format(target_dir))


scheduling_latency_fig, (scheduling_latency_ax1, scheduling_latency_ax2) = plt.subplots(2, 1, sharex=True)
scheduling_latency_fig.subplots_adjust(hspace=0.05)

scheduling_latency_fig.supxlabel('10 seconds')
scheduling_latency_ax1.set_ylabel("Diff from Random")
scheduling_latency_ax2.set_ylabel("Average Scheduling Latency in ms")

e2e_latency_fig, (e2e_latency_ax1, e2e_latency_ax2) = plt.subplots(2, 1, sharex=True)
e2e_latency_fig.subplots_adjust(hspace=0.05)
e2e_latency_fig.supxlabel('10 seconds')
e2e_latency_ax1.set_ylabel("Diff from Random")
e2e_latency_ax2.set_ylabel("Average Makespan (seconds)")

num_unfinished_tasks_fig, (num_unfinished_tasks_ax1, num_unfinished_tasks_ax2) = plt.subplots(2, 1, sharex=True)
num_unfinished_tasks_fig.subplots_adjust(hspace=0.05)
num_unfinished_tasks_fig.supxlabel('10 seconds')
num_unfinished_tasks_ax1.set_ylabel("Diff from Random")
num_unfinished_tasks_ax2.set_ylabel("Number of Unfinished Tasks")

plot_with_legend = True

for scheduler_type, nodes_metrics in schedulers_metrics.items():
    calibrated_e2e_latency_avg = [e2e_latency - schedulers_metrics['Random']["e2e_latency_avg"][i] for i, e2e_latency in
                                  enumerate(nodes_metrics["e2e_latency_avg"][:max_checkpoints])]
    scheduling_latency_ax1.plot(calibrated_e2e_latency_avg, label=scheduler_type)
    scheduling_latency_ax2.plot(nodes_metrics["e2e_latency_avg"][:max_checkpoints], label=scheduler_type)

    calibrated_task_e2e_makespan_duration_avg = [
        duration - schedulers_metrics['Random']["task_e2e_makespan_duration_avg"][i]
        for i, duration in enumerate(nodes_metrics["task_e2e_makespan_duration_avg"][:max_checkpoints])]
    e2e_latency_ax1.plot([duration / 1000 for duration in calibrated_task_e2e_makespan_duration_avg],
                         label=scheduler_type)
    e2e_latency_ax2.plot([duration / 1000 for duration in nodes_metrics["task_e2e_makespan_duration_avg"][:max_checkpoints]],
                         label=scheduler_type)

    calibrated_num_unfinished_tasks = [num_unfinished_task - schedulers_metrics['Random']["num_unfinished_tasks"][i]
                                       for i, num_unfinished_task in
                                       enumerate(nodes_metrics["num_unfinished_tasks"][:max_checkpoints])]
    num_unfinished_tasks_ax1.plot(calibrated_num_unfinished_tasks, label=scheduler_type)
    num_unfinished_tasks_ax2.plot(nodes_metrics["num_unfinished_tasks"][:max_checkpoints], label=scheduler_type)

if plot_with_legend:
     scheduling_latency_fig.legend(*scheduling_latency_ax1.get_legend_handles_labels(), loc='upper center', ncol=4, prop={'size': 9})
     num_unfinished_tasks_fig.legend(*num_unfinished_tasks_ax1.get_legend_handles_labels(), loc='upper center', ncol=4, prop={'size': 9})
scheduling_latency_fig.savefig("{}/scheduling_latency_avg.png".format(target_dir))
e2e_latency_fig.legend(*e2e_latency_ax1.get_legend_handles_labels(), loc='upper center', ncol=4, prop={'size': 9})
e2e_latency_fig.savefig("{}/e2e_latency_avg.png".format(target_dir))

num_unfinished_tasks_fig.savefig("{}/num_unfinished_tasks_avg.png".format(target_dir))


average_resource_fig, (average_resource_ax1, average_resource_ax2) = plt.subplots(2, 1, sharex=True)
average_resource_fig.subplots_adjust(hspace=0.05)
average_resource_ax1.set_ylabel("Diff from Random")
average_resource_ax2.set_ylabel("Average Resources Utilization")

var_resource_fig, (var_resource_ax1, var_resource_ax2) = plt.subplots(2, 1, sharex=True)
var_resource_fig.subplots_adjust(hspace=0.05)
var_resource_ax1.set_ylabel("Diff from Random")
var_resource_ax2.set_ylabel("Resources Variance")

for scheduler_type, nodes_metrics in all_nodes_metrics.items():

    calibrated_average_resource_mean = [resource - all_nodes_metrics['Random']["average_resource_mean"][i]
                                        for i, resource in enumerate(nodes_metrics["average_resource_mean"][:max_checkpoints])]
    average_resource_ax1.plot(calibrated_average_resource_mean, label=scheduler_type)
    average_resource_ax2.plot(nodes_metrics["average_resource_mean"][:max_checkpoints], label=scheduler_type)

    calibrated_var_resource_mean = [variance - all_nodes_metrics['Random']["var_resource_mean"][i]
                                    for i, variance in enumerate(nodes_metrics["var_resource_mean"][:max_checkpoints])]
    var_resource_ax1.plot(calibrated_var_resource_mean, label=scheduler_type)
    var_resource_ax2.plot(nodes_metrics["var_resource_mean"][:max_checkpoints], label=scheduler_type)

if plot_with_legend:
    average_resource_fig.legend(*average_resource_ax1.get_legend_handles_labels(), loc='upper center', ncol=4, prop={'size': 9})
    var_resource_fig.legend(*var_resource_ax1.get_legend_handles_labels(), loc='upper center', ncol=4, prop={'size': 9})

average_resource_fig.savefig("{}/average_resource_mean.png".format(target_dir))

var_resource_fig.savefig("{}/var_resource_mean.png".format(target_dir))
