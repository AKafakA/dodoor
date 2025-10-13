"""
PackingStrategy enum matching Java implementation.

Direct port from Java PackingStrategy.java to ensure exact behavioral matching.
"""

from enum import Enum


class PackingStrategy(Enum):
    """
    Packing strategies for task placement, matching Java implementation.
    
    Maps directly to edu.cam.dodoor.scheduler.taskplacer.PackingStrategy.java
    """
    
    SCORE = "score"       # Multi-dimensional load scoring with resource weights
    RIF = "rif"          # Running in FIFO - uses task count only  
    DURATION = "duration" # Total duration-based scoring
    NONE = "none"        # No optimization - random or fixed placement
    
    def __str__(self):
        return self.value