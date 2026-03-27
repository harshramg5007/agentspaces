"""DAG-related models for the Agent Space SDK."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List
from uuid import UUID

from .agent import _parse_timestamp


class EdgeType(str, Enum):
    """Types of edges in the DAG."""
    PARENT = "parent"
    SPAWNED = "spawned"
    DEPENDS_ON = "depends_on"


@dataclass
class DAGNode:
    """
    Represents a node in the DAG.
    
    Attributes:
        tuple_id: ID of the agent this node represents
        kind: Type/category of the agent
        status: Current status of the agent
        created_at: When the agent was created
        metadata: Additional metadata
    """
    tuple_id: UUID
    kind: str
    status: str
    created_at: datetime
    metadata: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DAGNode":
        """Create a DAGNode from a dictionary."""
        return cls(
            tuple_id=UUID(data["tuple_id"]),
            kind=data["kind"],
            status=data["status"],
            created_at=_parse_timestamp(data["created_at"]),
            metadata=data.get("metadata", {}),
        )


@dataclass
class Edge:
    """
    Represents an edge in the DAG.
    
    Attributes:
        from_node: Source node ID
        to_node: Target node ID
        type: Type of relationship
    """
    from_node: UUID
    to_node: UUID
    type: EdgeType

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Edge":
        """Create an Edge from a dictionary."""
        return cls(
            from_node=UUID(data["from"]),
            to_node=UUID(data["to"]),
            type=EdgeType(data["type"]),
        )


@dataclass
class DAG:
    """
    Represents a Directed Acyclic Graph of agents.
    
    Attributes:
        nodes: Dictionary mapping node IDs to DAGNode objects
        edges: List of edges connecting nodes
    """
    nodes: Dict[str, DAGNode] = field(default_factory=dict)
    edges: List[Edge] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DAG":
        """Create a DAG from a dictionary."""
        nodes = {}
        for node_id, node_data in data.get("nodes", {}).items():
            nodes[node_id] = DAGNode.from_dict(node_data)
        
        edges = []
        for edge_data in data.get("edges", []):
            edges.append(Edge.from_dict(edge_data))
        
        return cls(nodes=nodes, edges=edges)

    def get_children(self, node_id: UUID) -> List[UUID]:
        """Get all children of a node."""
        return [edge.to_node for edge in self.edges if edge.from_node == node_id]

    def get_parents(self, node_id: UUID) -> List[UUID]:
        """Get all parents of a node."""
        return [edge.from_node for edge in self.edges if edge.to_node == node_id]

    def get_roots(self) -> List[UUID]:
        """Get all root nodes (nodes with no parents)."""
        all_nodes = set(self.nodes.keys())
        has_parent = {str(edge.to_node) for edge in self.edges}
        return [UUID(node_id) for node_id in all_nodes if node_id not in has_parent]

    def get_leaves(self) -> List[UUID]:
        """Get all leaf nodes (nodes with no children)."""
        all_nodes = set(self.nodes.keys())
        has_child = {str(edge.from_node) for edge in self.edges}
        return [UUID(node_id) for node_id in all_nodes if node_id not in has_child]

    def topological_sort(self) -> List[UUID]:
        """Return nodes in topological order."""
        from collections import defaultdict, deque
        
        # Build adjacency list and in-degree count
        adj_list = defaultdict(list)
        in_degree = defaultdict(int)
        
        for edge in self.edges:
            adj_list[str(edge.from_node)].append(str(edge.to_node))
            in_degree[str(edge.to_node)] += 1
        
        # Initialize queue with nodes having no incoming edges
        queue = deque()
        for node_id in self.nodes:
            if in_degree[node_id] == 0:
                queue.append(node_id)
        
        # Process nodes
        result = []
        while queue:
            node_id = queue.popleft()
            result.append(UUID(node_id))
            
            # Reduce in-degree for neighbors
            for neighbor in adj_list[node_id]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        return result