package edu.cam.dodoor.node;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.TResourceVector;
import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TaskLauncherService {
    private final static Logger LOG = LoggerFactory.getLogger(TaskLauncherService.class);
    private Node _node;
    private NodeServiceMetrics _nodeServiceMetrics;
    private ThreadPoolExecutor _executor;
    private Map<String, String> _taskScriptPaths = new HashMap<>();
    private String _dockerImageName;
    private Map<String, List<Integer>> _taskCPURequirements;
    private Map<String, List<Integer>> _taskMemoryRequirements;
    private String _rootDir;

    /** A runnable that spins in a loop asking for tasks to launch and launching them. */
    private class TaskLaunchRunnable implements Runnable {
        TaskSpec _task;
        public TaskLaunchRunnable(TaskSpec task) {
            _task = task;
        }

        @Override
        public void run() {
            TaskSpec task = _task; // blocks until task is ready
            long waitingDuration = System.currentTimeMillis() - task._enqueuedTime;
            _nodeServiceMetrics.taskLaunched(waitingDuration);
            LOG.debug("Received task {}", task._taskId);
            try {
                Process process = executeLaunchTask(task);
                long pid = process.pid();
                LOG.debug("Task {} launched with pid {}", task._taskId, pid);
                if (task._taskType.equals("simulated")) {
                    Thread.sleep(task._duration);
                    process.destroyForcibly();
                    LOG.debug("Task {} completed", task._taskId);
                } else {
                    // Wait for the process to complete
                    int exitCode = process.waitFor();
                    if (exitCode != 0) {
                        LOG.error("Task {} failed with exit code {}", task._taskId, exitCode);
                    } else {
                        LOG.debug("Task {} completed successfully", task._taskId);
                    }
                }
                _node.taskFinished(task.getFullTaskId());
                _nodeServiceMetrics.taskFinished();
                BufferedReader stdError = new BufferedReader(new
                        InputStreamReader(process.getErrorStream()));
                if (!stdError.readLine().isEmpty()) {
                    LOG.error("Task {} failed to execute with error {} or unterminated", task._taskId, stdError.readLine());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            LOG.debug("Completed task {} on application backend at system time {}", task._taskId, System.currentTimeMillis());
        }

        /** Executes to launch a task */
        private Process executeLaunchTask(TaskSpec task) throws IOException, InterruptedException {
            Runtime rt = Runtime.getRuntime();
            String taskType = task._taskType;
            String command;
            if (taskType.equals(TaskTypeID.SIMULATED.toString())) {
                long memory = task._resourceVector.memory;
                long disks = task._resourceVector.disks;
                double cpu = task._resourceVector.cores;
                long duration = task._duration;
                long durationInSec = duration / 1000;
                command = generateStressCommand(cpu, memory, disks, durationInSec);
            } else {
                String scriptPath = _taskScriptPaths.get(taskType);
                String mode = task._mode;
                int modeIndex = TaskMode.getIndexFromName(mode);
                if (modeIndex < 0) {
                    LOG.error("Invalid task mode: {}", mode);
                    throw new IOException("Invalid task mode: " + mode);
                }
                int cpuCores = _taskCPURequirements.get(taskType).get(modeIndex);
                long memory = _taskMemoryRequirements.get(taskType).get(modeIndex);
                if (scriptPath == null) {
                    LOG.error("No script path found for task type {}", taskType);
                    throw new IOException("No script path found for task type " + taskType);
                }
                command = generatePythonCommand(scriptPath, _dockerImageName, cpuCores, memory,
                        _rootDir, mode);
            }
            return rt.exec(command);
        }
    }

    public void initialize(Configuration conf, int numSlot, NodeThrift nodeThrift,
                           JSONObject nodeTypeConfig, Map<String, String> taskScriptPaths,
                           Map<String, List<Integer>> taskCPURequirements,
                           Map<String, List<Integer>> taskMemoryRequirements) {
        /* The number of threads used by the service. */
        int numSlots = numSlot;
        if (numSlots <= 0) {
            // If the scheduler does not enforce a maximum number of tasks, just use a number of
            // threads equal to the number of cores.
            numSlots = Resources.getSystemCoresCapacity(nodeTypeConfig);
        }
        _node = nodeThrift._node;
        _nodeServiceMetrics = nodeThrift.getNodeServiceMetrics();
        _executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numSlots);
        _taskScriptPaths = taskScriptPaths;
        _taskCPURequirements = taskCPURequirements;
        _taskMemoryRequirements = taskMemoryRequirements;
        _dockerImageName = conf.getString(DodoorConf.DOCKER_IMAGE_NAME, DodoorConf.DEFAULT_DOCKER_IMAGE_NAME);
        _rootDir = conf.getString(DodoorConf.ROOT_DIR, DodoorConf.DEFAULT_ROOT_DIR);
        LOG.debug("Initializing task launcher service with {} slots", numSlots);
    }

    public void tryToLaunchTask(TaskSpec task) {
        TaskLaunchRunnable taskLaunchRunnable = new TaskLaunchRunnable(task);
        _executor.submit(taskLaunchRunnable);
    }

    public int getActiveTasks() {
        return _executor.getActiveCount();
    }

    public long getTaskCount() {
        return _executor.getTaskCount();
    }

    public long getCompletedTaskCount() {
        return _executor.getCompletedTaskCount();
    }

    public long getQueuedTaskCount() {
        return _executor.getQueue().size();
    }

    private static String generateStressCommand(double cores, long memory, long disks, long durationInSec) {
        if (Math.floor(cores) == cores) {
            int intCores = (int) cores;
            if (disks <= 0 && memory <= 0) {
                return String.format("stress -c %d --timeout %d", intCores, durationInSec);
            } else if (disks <= 0) {
                return String.format("stress -c %d --vm 1 --vm-bytes %dM --vm-hang %d --timeout %d",
                        intCores, memory, durationInSec, durationInSec);
            } else if (memory <= 0) {
                return String.format("stress -c %d --hdd 1 --hdd-bytes %dM --timeout %d", intCores, disks, durationInSec);
            } else {
                return String.format("stress -c %d --vm 1 --vm-bytes %dM --vm-hang %d -d 1 --hdd-bytes %dM --timeout %d",
                        intCores, memory, durationInSec, disks, durationInSec);
            }
        } else {
            // If cores is not an integer, we round it up to the next integer
            int intCores = (int) Math.ceil(cores);
            double load = cores / intCores; // load is the fraction of the core to be used
            if (disks <= 0 && memory <= 0) {
                return String.format("stress-ng -c %d -l %f --timeout %d",
                        intCores, load, durationInSec);
            } else if (disks <= 0) {
                return String.format("stress-ng -c %d -l %f --vm 1 --vm-bytes %dM --vm-hang %d --timeout %d",
                        intCores, load, memory, durationInSec, durationInSec);
            } else if (memory <= 0) {
                return String.format("stress-ng -c %d -l %f --hdd 1 --hdd-bytes %dM --timeout %d",
                        intCores, load, disks, durationInSec);
            } else {
                return String.format("stress-ng -c %d -l %f --vm 1 --vm-bytes %dM --vm-hang %d -d 1 --hdd-bytes %dM --timeout %d",
                        intCores, load, memory, durationInSec, disks, durationInSec);
            }
        }
    }

    private static String generatePythonCommand(String pythonScriptPath, String dockerImageName,
                                                int cpuCores, long memory, String hostDir, String taskMode) {
        String command = String.format("docker run -d --rm --cpus %d --memory %dm -v %s:/app %s python3 %s %s",
                cpuCores, memory, hostDir, dockerImageName, pythonScriptPath, taskMode);
        LOG.debug("Generated command: {}", command);
        return command;
    }
}
