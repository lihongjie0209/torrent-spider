"""Database module for SQLite storage."""

import sqlite3
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class TorrentDatabase:
    """SQLite database manager for torrent metadata."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.init_database()
    
    def init_database(self):
        """Initialize database connection and create tables."""
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self.create_tables()
            logger.info(f"Database initialized at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}", exc_info=True)
            raise
    
    def create_tables(self):
        """Create necessary tables if they don't exist."""
        try:
            cursor = self.conn.cursor()
            
            # Torrents table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS torrents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    info_hash TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    total_size INTEGER NOT NULL,
                    piece_length INTEGER,
                    num_pieces INTEGER,
                    comment TEXT,
                    created_date TEXT,
                    collected_at TEXT NOT NULL,
                    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Files table (one-to-many with torrents)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    torrent_id INTEGER NOT NULL,
                    path TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    FOREIGN KEY (torrent_id) REFERENCES torrents (id) ON DELETE CASCADE
                )
            """)
            
            # Create indexes
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_info_hash 
                ON torrents (info_hash)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_torrent_id 
                ON files (torrent_id)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_name 
                ON torrents (name)
            """)
            
            self.conn.commit()
            logger.info("Database tables created/verified")
            
        except Exception as e:
            logger.error(f"Failed to create tables: {e}", exc_info=True)
            raise
    
    def insert_torrent(
        self,
        info_hash: str,
        name: str,
        total_size: int,
        files: List[Dict[str, Any]],
        piece_length: Optional[int] = None,
        num_pieces: Optional[int] = None,
        comment: Optional[str] = None,
        created_date: Optional[str] = None,
        collected_at: Optional[str] = None
    ) -> bool:
        """
        Insert torrent and its files into database.
        
        Args:
            info_hash: Torrent info hash
            name: Torrent name
            total_size: Total size in bytes
            files: List of file dicts with 'path' and 'size'
            piece_length: Piece length in bytes
            num_pieces: Number of pieces
            comment: Torrent comment
            created_date: Creation date ISO string
            collected_at: Collection timestamp ISO string
            
        Returns:
            True if successful, False otherwise
        """
        try:
            cursor = self.conn.cursor()
            
            # Insert torrent
            cursor.execute("""
                INSERT INTO torrents (
                    info_hash, name, total_size, piece_length, 
                    num_pieces, comment, created_date, collected_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                info_hash, name, total_size, piece_length,
                num_pieces, comment, created_date,
                collected_at or datetime.utcnow().isoformat()
            ))
            
            torrent_id = cursor.lastrowid
            
            # Insert files
            if files:
                file_data = [
                    (torrent_id, file['path'], file['size'])
                    for file in files
                ]
                cursor.executemany("""
                    INSERT INTO files (torrent_id, path, size)
                    VALUES (?, ?, ?)
                """, file_data)
            
            self.conn.commit()
            logger.info(f"Inserted torrent {info_hash}: {name} ({len(files)} files)")
            return True
            
        except sqlite3.IntegrityError as e:
            logger.warning(f"Duplicate torrent {info_hash}: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to insert torrent {info_hash}: {e}", exc_info=True)
            self.conn.rollback()
            return False
    
    def torrent_exists(self, info_hash: str) -> bool:
        """
        Check if torrent already exists in database.
        
        Args:
            info_hash: Torrent info hash
            
        Returns:
            True if exists, False otherwise
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT 1 FROM torrents WHERE info_hash = ? LIMIT 1",
                (info_hash,)
            )
            return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking torrent existence: {e}", exc_info=True)
            return False
    
    def get_torrent_count(self) -> int:
        """Get total number of torrents in database."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM torrents")
            return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error getting torrent count: {e}", exc_info=True)
            return 0
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
