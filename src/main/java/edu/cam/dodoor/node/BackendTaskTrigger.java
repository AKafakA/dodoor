package edu.cam.dodoor.node;

import org.json.JSONObject;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;
import java.io.FileReader;

import edu.cam.dodoor.thrift.TResourceVector;

public class BackendTaskTrigger {
    private final String _taskTypeId;
    private final String _taskExecPath;
    // resource vector is a map of machine type to resource vector including cores, memory, and disks
    private final TResourceVector _resourceVector;
    private final Map<String, Long> _estimatedDurationMaps;
    private final static Logger LOG = LoggerFactory.getLogger(BackendTaskTrigger.class);

    public static Map<String, BackendTaskTrigger> getTaskTriggerMap(String taskConfigPath) {
        // read the task config file
        // parse the task config file
        // return a map of taskTypeId to BackendTaskTrigger
        Map<String, BackendTaskTrigger> taskTriggerMap = new HashMap<>();
        try {
            JSONObject taskConfig = new JSONObject(new FileReader(taskConfigPath));
            JSONArray taskConfigArray = taskConfig.getJSONArray("tasks");
            for (int i = 0; i < taskConfigArray.length(); i++) {
                JSONObject task = taskConfigArray.getJSONObject(i);
                String taskTypeId = task.getString("taskTypeId");
                String taskExecPath = task.getString("taskExecPath");
                JSONObject resourceVectorJson = task.getJSONObject("resourceVector");
                TResourceVector resourceVector = 
                    new TResourceVector(resourceVectorJson.getInt("cores"), 
                        resourceVectorJson.getInt("memory"), resourceVectorJson.getInt("disks"));
                Map<String, Long> estimatedDurationMaps = new HashMap<>();
                JSONObject estimatedDuration = task.getJSONObject("estimatedDuration");
                for (String machineType : estimatedDuration.keySet()) {
                    estimatedDurationMaps.put(machineType, estimatedDuration.getLong(machineType));
                }
                taskTriggerMap.put(taskTypeId, new BackendTaskTrigger(taskTypeId, taskExecPath, resourceVector, estimatedDurationMaps));
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading task config file", e);
        }
        return taskTriggerMap;
    }

    public BackendTaskTrigger(String taskTypeId, String taskExecPath, TResourceVector resourceVector, Map<String, Long> estimatedDurationMaps) {
        _taskTypeId = taskTypeId;
        _taskExecPath = taskExecPath;
        _resourceVector = resourceVector;
        _estimatedDurationMaps = estimatedDurationMaps;
    }

    public void triggerTask() throws Exception {
        // execute the python scripts under _taskExecPath
        long currentTime = System.currentTimeMillis();
        LOG.info("Triggering task: {}", _taskExecPath);
        ProcessBuilder processBuilder = new ProcessBuilder("python", _taskExecPath);
        processBuilder.start();
        LOG.info("Task triggered: {} in {} ms", _taskExecPath, System.currentTimeMillis() - currentTime);
    }

    public String getTaskTypeId() {
        return _taskTypeId;
    }

    public String getTaskExecPath() {
        return _taskExecPath;
    }

    public TResourceVector getResourceVector() {
        return _resourceVector;
    }

    public Map<String, Long> getEstimatedDurationMaps() {
        return _estimatedDurationMaps;
    }
}
