from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime

@dataclass
class ConflictResult:
    has_conflict: bool
    explanation: str
    conflicting_parts: List[str]
    analyzed_at: datetime
    chunk_ids: List[str]
    conflict_type: str = "unknown"  # content/chunk/document
    severity: str = "medium"  # high/medium/low
    contradictions: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self):
        return {
            "has_conflict": self.has_conflict,
            "explanation": self.explanation, 
            "conflicting_parts": self.conflicting_parts,
            "analyzed_at": self.analyzed_at.isoformat(),
            "chunk_ids": self.chunk_ids,
            "conflict_type": self.conflict_type,
            "severity": self.severity,
            "contradictions": self.contradictions
        }
        
    def get_contradiction_types(self) -> List[str]:
        """
        Get list of conflict types from detail data

        Returns:
        List[str]: List of conflict types (direct/indirect)
        """
        types = set()
        for contradiction in self.contradictions:
            if "type" in contradiction:
                types.add(contradiction["type"])
        return list(types)