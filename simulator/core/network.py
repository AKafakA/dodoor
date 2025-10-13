"""
Network delay modeling for distributed system simulation.

This module provides realistic network delay modeling with configurable
distributions to simulate communication latencies in distributed scheduling.
"""

import random
import logging
from typing import Dict, Tuple, Optional
from dataclasses import dataclass
from enum import Enum

try:
    from ..config.simulation_config import NetworkConfig
except ImportError:
    from config.simulation_config import NetworkConfig

logger = logging.getLogger(__name__)


class MessageType(Enum):
    """Types of messages in the distributed system."""
    
    # Scheduler messages
    SCHEDULE_REQUEST = "schedule_request"
    SCHEDULE_RESPONSE = "schedule_response"
    TASK_ASSIGNMENT = "task_assignment"
    
    # DataStore messages  
    LOAD_QUERY = "load_query"
    LOAD_RESPONSE = "load_response"
    LOAD_UPDATE = "load_update"
    LOAD_UPDATE_ACK = "load_update_ack"
    
    # Node messages
    HEARTBEAT = "heartbeat"
    TASK_STATUS = "task_status" 
    RESOURCE_UPDATE = "resource_update"
    
    # Generic messages
    RPC_CALL = "rpc_call"
    RPC_RESPONSE = "rpc_response"


class ComponentType(Enum):
    """Types of components in the distributed system."""
    SCHEDULER = "scheduler"
    DATASTORE = "datastore" 
    NODE = "node"
    CLIENT = "client"


@dataclass
class Message:
    """Represents a message being transmitted over the network."""
    message_id: str
    message_type: MessageType
    source_id: str
    source_type: ComponentType
    destination_id: str
    destination_type: ComponentType
    size_bytes: int = 1024  # Default message size
    timestamp_sent: float = 0.0
    timestamp_received: float = 0.0
    payload: Dict = None
    
    def __post_init__(self):
        if self.payload is None:
            self.payload = {}
    
    @property
    def latency_ms(self) -> float:
        """Calculate message latency in milliseconds."""
        return self.timestamp_received - self.timestamp_sent


class NetworkDelayModel:
    """
    Models network delays with configurable distributions and component-specific latencies.
    
    Supports:
    - Normal distribution delays with configurable mean and standard deviation
    - Component-specific latency configurations  
    - Message size-based delay modeling
    - Network congestion simulation
    """
    
    def __init__(self, config: NetworkConfig, seed: Optional[int] = None):
        """
        Initialize network delay model.
        
        Args:
            config: Network configuration parameters
            seed: Random seed for reproducible delays
        """
        self.config = config
        self._random = random.Random(seed)
        self._message_counter = 0
        
        # Track network statistics
        self._total_messages = 0
        self._total_latency = 0.0
        self._latency_samples = []
        
        logger.info(f"Initialized network model: mean={config.mean_latency_ms}ms, "
                   f"std={config.std_latency_ms}ms")
    
    def generate_latency(self, source_type: ComponentType, 
                        destination_type: ComponentType,
                        message_type: MessageType = MessageType.RPC_CALL,
                        message_size_bytes: int = 1024) -> float:
        """
        Generate network latency for a message between two components.
        
        Args:
            source_type: Type of source component
            destination_type: Type of destination component  
            message_type: Type of message being sent
            message_size_bytes: Size of message in bytes
            
        Returns:
            Network latency in milliseconds
        """
        
        # Get base latency from component-specific configuration
        base_latency = self._get_component_latency(source_type, destination_type)
        
        # Add random variation using normal distribution
        # Ensure latency is never negative
        random_variation = max(0, self._random.normalvariate(0, self.config.std_latency_ms))
        total_latency = base_latency + random_variation
        
        # Add message size-based delay (simple linear model)
        # Assume 10GB/s network bandwidth for size-based delay
        size_delay_ms = message_size_bytes / (1024 * 1024 * 1024) * 10000
        total_latency += size_delay_ms
        
        # Record statistics
        self._total_messages += 1
        self._total_latency += total_latency
        self._latency_samples.append(total_latency)
        
        # Keep only recent samples for rolling statistics  
        if len(self._latency_samples) > 10000:
            self._latency_samples = self._latency_samples[-5000:]
        
        logger.debug(f"Generated latency: {source_type.value} -> {destination_type.value} "
                    f"({message_type.value}): {total_latency:.3f}ms")
        
        return total_latency
    
    def _get_component_latency(self, source_type: ComponentType, 
                              destination_type: ComponentType) -> float:
        """Get component-specific base latency."""
        
        # Use specific latencies if configured
        if (source_type == ComponentType.SCHEDULER and 
            destination_type == ComponentType.NODE):
            return self.config.scheduler_to_node_ms
        elif (source_type == ComponentType.NODE and 
              destination_type == ComponentType.DATASTORE):
            return self.config.node_to_datastore_ms
        elif (source_type == ComponentType.SCHEDULER and 
              destination_type == ComponentType.DATASTORE):
            return self.config.scheduler_to_datastore_ms
        else:
            # Default to mean latency for other combinations
            return self.config.mean_latency_ms
    
    def create_message(self, message_type: MessageType, source_id: str, 
                      source_type: ComponentType, destination_id: str,
                      destination_type: ComponentType, 
                      size_bytes: int = 1024,
                      payload: Dict = None) -> Message:
        """
        Create a new message with unique ID.
        
        Args:
            message_type: Type of message
            source_id: ID of source component
            source_type: Type of source component
            destination_id: ID of destination component
            destination_type: Type of destination component
            size_bytes: Message size in bytes
            payload: Optional message payload
            
        Returns:
            New message instance
        """
        self._message_counter += 1
        message_id = f"msg_{self._message_counter:06d}"
        
        return Message(
            message_id=message_id,
            message_type=message_type,
            source_id=source_id,
            source_type=source_type,
            destination_id=destination_id,
            destination_type=destination_type,
            size_bytes=size_bytes,
            payload=payload or {}
        )
    
    def send_message(self, message: Message, current_time: float) -> Tuple[Message, float]:
        """
        Send a message and calculate when it will be received.
        
        Args:
            message: Message to send
            current_time: Current simulation time
            
        Returns:
            Tuple of (message, arrival_time)
        """
        # Generate network latency
        latency = self.generate_latency(
            message.source_type,
            message.destination_type, 
            message.message_type,
            message.size_bytes
        )
        
        # Calculate arrival time
        message.timestamp_sent = current_time
        message.timestamp_received = current_time + latency
        
        logger.debug(f"Sent {message.message_id}: {message.source_id} -> "
                    f"{message.destination_id} (arrives at {message.timestamp_received:.3f}ms)")
        
        return message, message.timestamp_received
    
    @property
    def average_latency(self) -> float:
        """Calculate average network latency across all messages."""
        return self._total_latency / self._total_messages if self._total_messages > 0 else 0.0
    
    @property
    def total_messages(self) -> int:
        """Total number of messages processed."""
        return self._total_messages
    
    def get_latency_statistics(self) -> Dict[str, float]:
        """Get comprehensive latency statistics."""
        if not self._latency_samples:
            return {}
        
        samples = sorted(self._latency_samples)
        n = len(samples)
        
        return {
            'count': n,
            'mean': sum(samples) / n,
            'min': samples[0],
            'max': samples[-1],
            'p50': samples[n // 2],
            'p95': samples[int(n * 0.95)] if n > 20 else samples[-1],
            'p99': samples[int(n * 0.99)] if n > 100 else samples[-1],
        }
    
    def reset_statistics(self):
        """Reset network statistics counters."""
        self._total_messages = 0
        self._total_latency = 0.0
        self._latency_samples.clear()
        logger.info("Reset network statistics")


class NetworkSimulator:
    """
    High-level network simulator that manages message routing and delivery.
    
    Provides realistic network behavior including:
    - Message queuing and ordering
    - Latency simulation
    - Message loss simulation (optional)
    - Bandwidth modeling (optional)
    """
    
    def __init__(self, delay_model: NetworkDelayModel):
        """Initialize network simulator."""
        self.delay_model = delay_model
        self._pending_messages: Dict[str, Message] = {}
        
    def send_message_async(self, message_type: MessageType, source_id: str,
                          source_type: ComponentType, destination_id: str, 
                          destination_type: ComponentType, current_time: float,
                          payload: Dict = None, size_bytes: int = 1024) -> Tuple[str, float]:
        """
        Send a message asynchronously and return arrival time.
        
        Returns:
            Tuple of (message_id, arrival_time)
        """
        message = self.delay_model.create_message(
            message_type, source_id, source_type,
            destination_id, destination_type, size_bytes, payload
        )
        
        message, arrival_time = self.delay_model.send_message(message, current_time)
        self._pending_messages[message.message_id] = message
        
        return message.message_id, arrival_time
    
    def get_message(self, message_id: str) -> Optional[Message]:
        """Retrieve a message by ID."""
        return self._pending_messages.get(message_id)
    
    def complete_message(self, message_id: str) -> Optional[Message]:
        """Mark message as delivered and remove from pending."""
        return self._pending_messages.pop(message_id, None)
    
    def get_statistics(self) -> Dict[str, any]:
        """Get comprehensive network statistics."""
        return {
            'total_messages': self.delay_model.total_messages,
            'average_latency_ms': self.delay_model.average_latency,
            'pending_messages': len(self._pending_messages),
            'latency_distribution': self.delay_model.get_latency_statistics()
        }