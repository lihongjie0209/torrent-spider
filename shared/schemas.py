"""JSON message schemas for Kafka topics."""

import json
from datetime import datetime
from typing import Dict, List, Any, Optional


class TorrentHashMessage:
    """Schema for torrent-hashes topic."""
    
    def __init__(self, info_hash: str, discovered_at: Optional[str] = None):
        self.info_hash = info_hash
        self.discovered_at = discovered_at or datetime.utcnow().isoformat()
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps({
            "info_hash": self.info_hash,
            "discovered_at": self.discovered_at
        })
    
    @staticmethod
    def from_json(data: str) -> 'TorrentHashMessage':
        """Create from JSON string."""
        obj = json.loads(data)
        return TorrentHashMessage(
            info_hash=obj["info_hash"],
            discovered_at=obj.get("discovered_at")
        )


class TorrentMetadataMessage:
    """Schema for torrent-metadata topic."""
    
    def __init__(
        self,
        info_hash: str,
        name: str,
        files: List[Dict[str, Any]],
        total_size: int,
        created_date: Optional[str] = None,
        piece_length: Optional[int] = None,
        num_pieces: Optional[int] = None,
        comment: Optional[str] = None
    ):
        self.info_hash = info_hash
        self.name = name
        self.files = files
        self.total_size = total_size
        self.created_date = created_date
        self.piece_length = piece_length
        self.num_pieces = num_pieces
        self.comment = comment
        self.collected_at = datetime.utcnow().isoformat()
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps({
            "info_hash": self.info_hash,
            "name": self.name,
            "files": self.files,
            "total_size": self.total_size,
            "created_date": self.created_date,
            "piece_length": self.piece_length,
            "num_pieces": self.num_pieces,
            "comment": self.comment,
            "collected_at": self.collected_at
        })
    
    @staticmethod
    def from_json(data: str) -> 'TorrentMetadataMessage':
        """Create from JSON string."""
        obj = json.loads(data)
        return TorrentMetadataMessage(
            info_hash=obj["info_hash"],
            name=obj["name"],
            files=obj["files"],
            total_size=obj["total_size"],
            created_date=obj.get("created_date"),
            piece_length=obj.get("piece_length"),
            num_pieces=obj.get("num_pieces"),
            comment=obj.get("comment")
        )
