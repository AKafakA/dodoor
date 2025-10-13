package edu.cam.dodoor.node;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.TResourceVector;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TaskMapsPerNodeType {
    public static final Logger LOG = LoggerFactory.getLogger(TaskMapsPerNodeType.class);
    public Map<String, Map<String, TResourceVector>> _resourceVectorMap;
    public Map<String, Map<String, Long>> _taskDurations;

    public static Map<String, TaskMapsPerNodeType> createTaskMapsPerNodeTypeMap(JSONObject taskTypeConfig,
                                                                                JSONObject nodeConfig) {
        Map<String, TaskMapsPerNodeType> taskMapsPerNodeTypeMap = new HashMap<>();
        Set<String> nodeTypeIds = new HashSet<>();
        for (int i = 0; i < nodeConfig.length(); i++) {
            JSONArray nodeTypes = nodeConfig.getJSONArray(DodoorConf.NODE_TYPE_LIST_KEY);
            for (int j = 0; j < nodeTypes.length(); j++) {
                JSONObject nodeTypeJson = nodeTypes.getJSONObject(j);
                String nodeTypeId = nodeTypeJson.getString(DodoorConf.NODE_TYPE);
                nodeTypeIds.add(nodeTypeId);
            }
        }

        for (String nodeTypeId : nodeTypeIds) {
            TaskMapsPerNodeType taskMapsPerNodeType = new TaskMapsPerNodeType(nodeTypeId, taskTypeConfig);
            taskMapsPerNodeTypeMap.put(nodeTypeId, taskMapsPerNodeType);
        }
        return taskMapsPerNodeTypeMap;
    }

    public TaskMapsPerNodeType(String nodeTypeId, JSONObject taskTypeConfig) {
        JSONArray taskTypes = taskTypeConfig.getJSONArray("tasks");
        _resourceVectorMap = new HashMap<>();
        _taskDurations = new HashMap<>();
        for (int i = 0; i < taskTypes.length(); i++) {
            JSONObject taskTypeJson = taskTypes.getJSONObject(i);
            String taskType = taskTypeJson.getString("taskTypeId");
            JSONObject instanceInfo = taskTypeJson.getJSONObject("instanceInfo");
            for (String nodeType : instanceInfo.keySet()) {
                if (nodeType.equals(nodeTypeId)) {
                    JSONObject taskNodeTypeJson = instanceInfo.getJSONObject(nodeType);
                    JSONObject taskResourcesJson = taskNodeTypeJson.getJSONObject("resourceVector");
                    JSONArray coresArray = taskResourcesJson.getJSONArray("cores");
                    JSONArray memoryArray = taskResourcesJson.getJSONArray("memory");
                    JSONArray diskArray = taskResourcesJson.getJSONArray("disks");
                    JSONArray durationsArray = taskNodeTypeJson.getJSONArray("estimatedDuration");
                    for (int j = 0; j < coresArray.length(); j++) {
                        // assuming all resource share the same number of modes
                        double cores = coresArray.getDouble(j);
                        long memory = memoryArray.getLong(j);
                        long disks = diskArray.getLong(j);
                        String mode = TaskMode.getNameFromIndex(j);
                        if (!_resourceVectorMap.containsKey(taskType)) {
                            _resourceVectorMap.put(taskType, new HashMap<>());
                        }
                        _resourceVectorMap.get(taskType).put(mode,
                                new TResourceVector(cores, memory, disks));

                        long duration = durationsArray.getLong(j);
                        if (!_taskDurations.containsKey(taskType)) {
                            _taskDurations.put(taskType, new HashMap<>());
                        }
                        _taskDurations.get(taskType).put(mode, duration);
                        LOG.debug("NodeType: {}, TaskType: {}, Mode: {}, ResourceVector: {}, Duration: {}",
                                new Object[]{nodeType, taskType, mode, _resourceVectorMap.get(taskType),
                                _taskDurations.get(taskType)});
                    }
                }
            }
        }
    }

    public TResourceVector getResourceVector(String taskType, String mode) {
        return _resourceVectorMap.get(taskType).get(mode);
    }

    public long getTaskDuration(String taskType, String mode) {
        return _taskDurations.get(taskType).get(mode);
    }
}
