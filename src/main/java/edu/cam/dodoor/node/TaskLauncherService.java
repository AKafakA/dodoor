package edu.cam.dodoor.node;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.TResourceVector;
import edu.cam.dodoor.utils.*;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TaskLauncherService {
    private final static Logger LOG = LoggerFactory.getLogger(TaskLauncherService.class);
    private Node _node;
    private NodeServiceMetrics _nodeServiceMetrics;
    private ThreadPoolExecutor _executor;
    private Map<String, String> _taskScriptPaths = new HashMap<>();
    private String _dockerImageName;
    private Map<String, List<Double>> _taskCPURequirements;
    private Map<String, List<Integer>> _taskMemoryRequirements;
    private String _rootDir;

    /** A runnable that spins in a loop asking for tasks to launch and launching them. */
    private class TaskLaunchRunnable implements Runnable {
        TaskSpec _task;
        public TaskLaunchRunnable(TaskSpec task) {
            _task = task;
        }

        /**
         * IMPROVEMENT: Terminates the entire process tree (parent and all children).
         * This is crucial for commands like stress-ng that spawn worker processes.
         * @param process The process to terminate.
         */
        private void terminateProcessTree(Process process) {
            long pid = process.pid();
            LOG.debug("Attempting to terminate process tree for PID: {}", pid);
            try {
                // Get a handle to the process
                ProcessHandle processHandle = process.toHandle();

                // First, destroy all descendant processes (the children)
                Stream<ProcessHandle> descendants = processHandle.descendants();
                descendants.forEach(ph -> {
                    LOG.debug("Destroying child process PID: {}", ph.pid());
                    ph.destroyForcibly();
                });

                // Finally, destroy the main parent process
                LOG.debug("Destroying parent process PID: {}", pid);
                processHandle.destroyForcibly();

            } catch (Exception e) {
                LOG.error("Error during process tree termination for PID {}", pid, e);
            }
        }

        @Override
        public void run() {
            long waitingDuration = System.currentTimeMillis() - _task._enqueuedTime;
            _nodeServiceMetrics.taskLaunched(waitingDuration);
            LOG.debug("Received task {} which waited {} ms", _task._taskId, waitingDuration);

            Process process = null;
            long taskStartTime = System.currentTimeMillis();
            try {
                process = executeLaunchTask(_task);
                // Wait for the process to complete, with a timeout
                boolean finishedInTime = process.waitFor(_task._durationInMs, TimeUnit.MILLISECONDS);

                if (!finishedInTime) {
                    LOG.warn("Task {} timed out after {} ms. Forcibly terminating.",
                            _task._taskId, _task._durationInMs);
                } else {
                    int exitCode = process.exitValue();
                    LOG.debug("Task {} finished with exit code {}", _task._taskId, exitCode);
                    if (exitCode != 0) {
                        LOG.error("Task {} failed with a non-zero exit code: {}", _task._taskId, exitCode);
                    }
                }
            } catch (IOException | InterruptedException e) {
                LOG.error("Failed to execute or wait for task {}", _task._taskId, e);
                Thread.currentThread().interrupt();
            } finally {
                if (process != null) {
                    // This now reliably kills stress-ng and all its workers.
                    terminateProcessTree(process);
                }
                try {
                    _node.taskFinished(_task.getFullTaskId());
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
                _nodeServiceMetrics.taskFinished();
                long taskEndTime = System.currentTimeMillis();
                LOG.debug("Task {} processing completed in {} ms", _task._taskId, taskEndTime - taskStartTime);
            }
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
                long durationInMs = task._durationInMs;
                double durationInSec = (durationInMs * 1.0) / 1000;
                command = generateStressCommand(cpu, memory, disks, durationInSec);
            } else {
                String scriptPath = _taskScriptPaths.get(taskType);
                String mode = task._mode;
                int modeIndex = TaskMode.getIndexFromName(mode);
                if (modeIndex < 0) {
                    LOG.error("Invalid task mode: {}", mode);
                    throw new IOException("Invalid task mode: " + mode);
                }
                double cpuCores = _taskCPURequirements.get(taskType).get(modeIndex);
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
                           Map<String, List<Double>> taskCPURequirements,
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

    private static String generateStressCommand(double cores, long memory, long disks, double durationInSec) {
        String stressCommand;
        if (Math.floor(cores) == cores) {
            int intCores = (int) cores;
            if (disks <= 0 && memory <= 0) {
                stressCommand = String.format("stress -c %d --timeout %f", intCores, durationInSec);
            } else if (disks <= 0) {
                stressCommand =  String.format("stress -c %d --vm 1 --vm-bytes %dM --timeout %f",
                        intCores, memory, durationInSec);
            } else if (memory <= 0) {
                stressCommand =  String.format("stress -c %d --hdd 1 --hdd-bytes %dM --timeout %f", intCores, disks, durationInSec);
            } else {
                stressCommand =  String.format("stress -c %d --vm 1 --vm-bytes %dM -d 1 --hdd-bytes %dM --timeout %f",
                        intCores, memory, disks, durationInSec);
            }
        } else {
            // If cores is not an integer, we round it up to the next integer
            int intCores = (int) Math.ceil(cores);
            double load = cores / intCores;
            int loadPercentage = (int) (load * 100);
            if (disks <= 0 && memory <= 0) {
                stressCommand =  String.format("stress-ng -c %d -l %d --timeout %f",
                        intCores, loadPercentage, durationInSec);
            } else if (disks <= 0) {
                stressCommand =  String.format("stress-ng -c %d -l %d --vm 1 --vm-bytes %dM --timeout %f",
                        intCores, loadPercentage, memory, durationInSec);
            } else if (memory <= 0) {
                stressCommand =  String.format("stress-ng -c %d -l %d --hdd 1 --hdd-bytes %dM --timeout %f",
                        intCores, loadPercentage, disks, durationInSec);
            } else {
                stressCommand =  String.format("stress-ng -c %d -l %d --vm 1 --vm-bytes %dM -d 1 --hdd-bytes %dM --timeout %f",
                        intCores, loadPercentage, memory, disks, durationInSec);
            }
        }
        return stressCommand;
    }

    private static String generatePythonCommand(String pythonScriptPath, String dockerImageName,
                                                double cpuCores, long memory, String hostDir, String taskMode) {
        String command = String.format("docker run -d --rm --cpus %d --memory %dm -v %s:/app %s python3 %s %s",
                (int)cpuCores, memory, hostDir, dockerImageName, pythonScriptPath, taskMode);
        return command;
    }
}
