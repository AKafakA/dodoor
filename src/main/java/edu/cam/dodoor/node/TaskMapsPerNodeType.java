package edu.cam.dodoor.node;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.TResourceVector;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TaskMapsPerNodeType {
    public String _nodeTypeId;
    public Map<String, TResourceVector> _resourceVectorMap;
    public Map<String, Long> _taskDurations;

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
            for (String nodeType : taskTypeJson.keySet()) {
                if (nodeType.equals(nodeTypeId)) {
                    JSONObject taskNodeTypeJson = taskTypeJson.getJSONObject(nodeType);
                    TResourceVector resourceVector = new TResourceVector(
                            taskNodeTypeJson.getDouble("cpu"),
                            taskNodeTypeJson.getLong("memory"),
                            taskNodeTypeJson.getLong("disk")
                    );
                    _resourceVectorMap.put(taskType, resourceVector);
                    _taskDurations.put(taskType, taskNodeTypeJson.getLong("estimatedDuration"));
                }
            }
        }
    }

    public TResourceVector getResourceVector(String taskType) {
        return _resourceVectorMap.get(taskType);
    }

    public long getTaskDuration(String taskType) {
        return _taskDurations.getOrDefault(taskType, 0L);
    }
}
