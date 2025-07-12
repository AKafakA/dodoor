package edu.cam.dodoor.node;

public enum TaskMode {
    SMALL("small", 0), // Small task, used for testing purposes
    MEDIUM("medium", 1), // Medium task, used for testing purposes
    LONG("long", 2); // Long-running task, used for testing purposes

    // Task that involves lin packing operations
    private final String _name;
    private final int _index;

    private TaskMode(String name, int index) {
        // Constructor is private to prevent instantiation
        _name = name;
        _index = index;
    }

    public String getName() {
        return _name;
    }
    public int getIndex() {
        return _index;
    }

    public boolean equalsName(String otherName) {
        // (otherName == null) check is not needed because name.equals(null) returns false
        return _name.equals(otherName);
    }

    public static int getIndexFromName(String name) {
        for (TaskMode mode : TaskMode.values()) {
            if (mode.getName().equals(name)) {
                return mode.getIndex();
            }
        }
        return -1; // Return -1 if no match found
    }
}
