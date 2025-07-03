package edu.cam.dodoor.node;

public enum TaskTypeID {
    SIMULATED("simulated"), // Simulated task, used for testing purposes
    FLOAT_OPERATION("float_operation"), // Task that performs floating-point operations
    LIN_PACK("linpack"), // Task that involves lin packing operations
    MATMUL("matmul"),
    IMAGE_PROCESSING("image_processing"), // Task that processes images
    VIDEO_PROCESSING("video_processing"), // Task that processes videos
    MAP_REDUCE("map_reduce"), // Task that performs map-reduce operations
    CHAMELEON("chameleon"), // Task that runs the Chameleon benchmark
    PYAES("pyaes"), // Task that runs the PyAES benchmark
    FEATURE_GENERATION("feature_generation"), // Task that generates features
    MODEL_TRAINING("model_training"), // Task that trains a machine learning model
    MODEL_SERVING("model_serving"); // Task that serves a machine learning model


    private final String _name;
    private TaskTypeID(String name) {
        // Constructor is private to prevent instantiation
        _name = name;
    }
    public boolean equalsName(String otherName) {
        // (otherName == null) check is not needed because name.equals(null) returns false
        return _name.equals(otherName);
    }

    public String toString() {
        return _name;
    }
}
