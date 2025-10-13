"""
TaskTypeID enum matching Java implementation.

Direct port from Java TaskTypeID.java enum.
"""

from enum import Enum


class TaskTypeID(Enum):
    """
    Task type identifiers matching Java implementation.
    
    Maps directly to edu.cam.dodoor.node.TaskTypeID.java
    """
    
    SIMULATED = "simulated"  # Special case - uses provided resource requirements
    WEB_SERVER = "web_server"
    ML_TRAINING = "ml_training"
    DATA_PROCESSING = "data_processing"
    BATCH_JOB = "batch_job"
    
    # Add more task types as needed from configuration
    
    def __str__(self):
        return self.value
    
    @classmethod
    def is_simulated(cls, task_type: str) -> bool:
        """Check if task type is SIMULATED (uses provided resources, not mapped)."""
        return task_type == cls.SIMULATED.value